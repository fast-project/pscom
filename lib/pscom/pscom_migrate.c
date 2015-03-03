/*
 * ParaStation
 *
 * Copyright (C) 2007 ParTec Cluster Competence Center GmbH, Munich
 *
 * This file may be distributed under the terms of the Q Public License
 * as defined in the file LICENSE.QPL included in the packaging of this
 * file.
 *
 * Author:	Simon Pickartz <spickartz@eonerc.rwth-aachen.de>
 */

#include "pscom_migrate.h"
#include "pscom_con.h"
#include "pscom_str_util.h"
#include "pscom_io.h"
#include "pscom_queues.h"
#include "pscom_req.h"
#include "pslib.h"
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <netinet/tcp.h>
#include <errno.h>

#define _PSCOM_SUPPORT_MIGRATION

#ifdef _PSCOM_SUPPORT_MIGRATION

#define PSCOM_MOSQUITTO_CLIENT_NAME_LENGTH 	50	
#define PSCOM_MOSQUITTO_TOPIC_LENGTH 		50	
#define PSCOM_MOSQUITTO_TOPIC 			"_migration_req"
#define PSCOM_MOSQUITTO_RESP_TOPIC 		"migration_resp"
#define PSCOM_BROKER_HOST 			"localhost"
#define PSCOM_BROKER_PORT 			1883
#define PSCOM_KEEP_ALIVE_INT 			60

static int pscom_mosquitto_initialized;
static struct mosquitto *pscom_mosquitto_client;

static
int pscom_suspend_plugins(void)
{	
	struct list_head *pos_sock;
	struct list_head *pos_con;
	int arch;
	pscom_plugin_t *plugin;

	/*
	 * Shutdown connections first
	 */	

	/* iterate over all sockets */
	list_for_each(pos_sock, &pscom.sockets) {
		pscom_sock_t *sock = list_entry(pos_sock, pscom_sock_t, next);

		/* iterate over all connections */
		list_for_each(pos_con, &sock->connections) {
			pscom_con_t *con = list_entry(pos_con,
			    			      pscom_con_t, 
						      next);
	
			/* determine corresponding plugin */
			arch = PSCOM_CON_TYPE2ARCH(con->pub.type);
		 	plugin = pscom_plugin_by_archid(arch);

			/* go to next connection if plugin not set */
			if (plugin == NULL)
				continue;

			/* shutdown the connection if not migratable */
			if (plugin->properties & 
			    PSCOM_PLUGIN_PROP_NOT_MIGRATABLE) {
			
				pscom_con_shutdown(con);	

			
				/* wait for response */
				while (con->read_is_suspended == 0) {
					pscom_poll_read_start(con);
					pscom_test_any();
				}
			}
		}
	}

	/*
	 * Shutdown non-migratable plugins
	 */

	DPRINT(1, "%s %u: Find non-migratable plugins ...", __FILE__, __LINE__);
	struct list_head *pos_plugin;
	list_for_each(pos_plugin, &pscom_plugins) {
		plugin = list_entry(pos_plugin, pscom_plugin_t, next);

		if (plugin->properties & PSCOM_PLUGIN_PROP_NOT_MIGRATABLE) {
			DPRINT(1, 
			       "%s %u: Destroying '%s' ...", 
			       __FILE__, 
			       __LINE__,
			       plugin->name);
			//plugin->destroy();
			DPRINT(1, 
			       "%s %u: Successfully destroyed '%s'!", 
			       __FILE__, 
			       __LINE__,
			       plugin->name);
		}
	}

	return 0;
}


int pscom_suspend_non_migratable_plugins(void)
{
	int ret;
	pscom_lock(); {
		ret = pscom_suspend_plugins();
	} pscom_unlock();
	return ret;
}


static
int pscom_resume_plugins(void)
{
	struct list_head *pos_sock;
	struct list_head *pos_con;
	int arch;
	pscom_plugin_t *plugin;

	/* iterate over all sockets */
	list_for_each(pos_sock, &pscom.sockets) {
		pscom_sock_t *sock = list_entry(pos_sock, pscom_sock_t, next);

		/* iterate over all connections */
		list_for_each(pos_con, &sock->connections) {
			pscom_con_t *con = list_entry(pos_con,
			    			      pscom_con_t, 
						      next);
	
			/* resume connections */
			pscom_resume_connection(&con->pub);	
		}
	}

	return PSCOM_SUCCESS;
}

int pscom_resume_non_migratable_plugins(void)
{
	pscom_lock(); {
		pscom_resume_plugins();
	} pscom_unlock();

	return PSCOM_SUCCESS;
}

static
void pscom_message_callback(struct mosquitto *mosquitto_client, 
    				 void *arg, 
				 const struct mosquitto_message *message)
{
	char my_pid[10];
	sprintf(my_pid, "%d", getpid());

	DPRINT(1, "\nINFO: Got MQTT message: %s (vs %s = my pid)\n", (char*)message->payload, my_pid);

	if(strncmp((char*)message->payload, my_pid, 10)==0) {

		if (pscom.migration_state == PSCOM_MIGRATION_INACTIVE) {
			pscom.migration_state = PSCOM_MIGRATION_REQ;
			DPRINT(2, "\nSTATE: PSCOM_MIGRATION_INACTIVE -> PSCOM_MIGRATION_REQ");
		} else if (pscom.migration_state == PSCOM_MIGRATION_IN_PROGRESS) {
			pscom.migration_state = PSCOM_MIGRATION_DONE;
			DPRINT(2, "STATE: PSCOM_MIGRATION_IN_PROGRESS -> PSCOM_MIGRATION_DONE");
		} else if (pscom.migration_state == PSCOM_MIGRATION_INACTIVE) {
			DPRINT(2, "STATE: PSCOM_MIGRATION_INACTIVE");
		} else if (pscom.migration_state == PSCOM_MIGRATION_DONE) {
			DPRINT(2, "STATE: PSCOM_MIGRATION_DONE");
		} else {
			DPRINT(2, "STATE: !UNKNOWN!");
		}
	}
}

void pscom_migration_handle_resume_req(void)
{
	DPRINT(1, "INFO: Handling resume request ...\n");
	pscom_resume_non_migratable_plugins();
	DPRINT(1, "INFO: Resume Complete!\n");

	/* reset migration state */
	pscom.migration_state = PSCOM_MIGRATION_INACTIVE;
}

void pscom_migration_handle_shutdown_req(void)
{
	/* change migration state */
	pscom.migration_state = PSCOM_MIGRATION_IN_PROGRESS;

	DPRINT(1, "INFO: Handling shutdown request ...\n");
	pscom_suspend_non_migratable_plugins();
	DPRINT(1, "INFO: Shutdown complete!\n");

	/* inform migration-framework */
	int err = mosquitto_publish(pscom_mosquitto_client,
	    			    NULL,
				    PSCOM_MOSQUITTO_RESP_TOPIC,
				    sizeof(pscom.migration_state), 
				    (const void*)&pscom.migration_state,
				    0,
				    false);
	if (err != MOSQ_ERR_SUCCESS) {
		fprintf(stderr, "ERROR: Could not publish on '%s' - %d"
		       "(%d [%s])", 
		       PSCOM_MOSQUITTO_RESP_TOPIC,
		       err,
		       errno, 
		       strerror(errno));
		exit(-1);
	}

	/* wait until the migration has terminated */
	while (pscom.migration_state != PSCOM_MIGRATION_DONE) {
		sched_yield();
	}

	/* resume the connections now */	
	pscom_migration_handle_resume_req();
}

int pscom_migration_init(void)
{
	pscom_mosquitto_initialized = 0;
	
	/* initialize libmosquitto */
	if (mosquitto_lib_init() != MOSQ_ERR_SUCCESS) {
		DPRINT(1, "%s %d: ERROR: Could not init libmosquitto ",
		       __FILE__, __LINE__);
		return PSCOM_ERR_STDERROR;
	}

	/* create a new mosquitto client */	
	char client_name[PSCOM_MOSQUITTO_CLIENT_NAME_LENGTH];
	char my_pid[10];
	sprintf(my_pid, "_%d", getpid());
	gethostname(client_name, PSCOM_MOSQUITTO_TOPIC_LENGTH);
	strcat(client_name, my_pid);
	pscom_mosquitto_client = mosquitto_new(client_name, 
	    				       true,
					       NULL);
	if (pscom_mosquitto_client == NULL) {
		DPRINT(1, "%s %d: ERROR: Could create new mosquitto client "
		       "instance (%d [%s])", 
		       __FILE__, __LINE__,
		       errno, 
		       strerror(errno));
		return PSCOM_ERR_STDERROR;
	}
	
	/* connect to mosquitto broker in BLOCKING manner */
	int err;
	err = mosquitto_connect(pscom_mosquitto_client,
				PSCOM_BROKER_HOST,
				PSCOM_BROKER_PORT,
			      	PSCOM_KEEP_ALIVE_INT);
	if ( err != MOSQ_ERR_SUCCESS) {
		DPRINT(1, "%s %d: ERROR: Could connect to the broker - %d"
		       "(%d [%s])", 
		       __FILE__, __LINE__,
		       err,
		       errno, 
		       strerror(errno));
		return PSCOM_ERR_STDERROR;
	} else {
		DPRINT(1, "Connected to the Mosquitto broker");
	}

	/* subscribe to the migration command topic */
	char topic[PSCOM_MOSQUITTO_TOPIC_LENGTH];
	gethostname(topic, PSCOM_MOSQUITTO_TOPIC_LENGTH);
	strcat(topic, PSCOM_MOSQUITTO_TOPIC);
	err = mosquitto_subscribe(pscom_mosquitto_client,
				  NULL,
				  topic,
				  0);
	if (err != MOSQ_ERR_SUCCESS) {
		DPRINT(1, "%s %d: ERROR: Could not subscribe to '%s' - %d"
		       "(%d [%s])", 
		       __FILE__, __LINE__,
		       topic,
		       err,
		       errno, 
		       strerror(errno));
		return PSCOM_ERR_STDERROR;
	} else {
		DPRINT(1, "Sucessfuly subscribed to '%s'", topic);
	}

	/* set the subscription callback */
	mosquitto_message_callback_set(pscom_mosquitto_client,
				       &pscom_message_callback);
	
	/* start the communication loop */
	err = mosquitto_loop_start(pscom_mosquitto_client);
	if ( err != MOSQ_ERR_SUCCESS) {
		DPRINT(1, "%s %d: ERROR: Could not start the communication "
		       "loop - %d",
		       __FILE__, __LINE__,
		       err);
		return PSCOM_ERR_STDERROR;
	}

	pscom_mosquitto_initialized = 1;
	return PSCOM_SUCCESS;
}

int pscom_migration_cleanup(void)
{
	int err;

	/* unsubscribe from the migration command topic */	
	err = mosquitto_unsubscribe(pscom_mosquitto_client,
				    NULL,
				    PSCOM_MOSQUITTO_TOPIC);
	if (err != MOSQ_ERR_SUCCESS) {
		DPRINT(1, "%s %d: ERROR: Could not unsubscribe from '%s' - %d"
		       "(%d [%s])", 
		       __FILE__, __LINE__,
		       PSCOM_MOSQUITTO_TOPIC,
		       err,
		       errno, 
		       strerror(errno));
		return PSCOM_ERR_STDERROR;
	}


	/* disconnect from broker */
	err = mosquitto_disconnect(pscom_mosquitto_client);
	if (err != MOSQ_ERR_SUCCESS) {
		DPRINT(1, "%s %d: ERROR: Could not disconnect from broker "
		       "- %d",
		       __FILE__, __LINE__,
		       err);
		return PSCOM_ERR_STDERROR;
	}

	/* stop the communication loop */
	err = mosquitto_loop_stop(pscom_mosquitto_client, false);
	if (err != MOSQ_ERR_SUCCESS) {
		DPRINT(1, "%s %d: ERROR: Could not stop the communication loop "
		       "- %d",
		       __FILE__, __LINE__,
		       err);
		return PSCOM_ERR_STDERROR;
	}

	/* destroy the mosquitto client */	
	mosquitto_destroy(pscom_mosquitto_client);

	/* cleanup libmosquitto */
	if (mosquitto_lib_cleanup() != MOSQ_ERR_SUCCESS) {
		DPRINT(1, "%s %d: ERROR: Could not cleanup libmosquitto ",
		       __FILE__, __LINE__);
	}

	return PSCOM_SUCCESS;
}

#else
int pscom_migration_init(void)
{
	return 0;
}
int pscom_migration_cleanup(void)
{
	return 0;
}
void pscom_migration_handle_shutdown_req(void)
{
	return;
}
#endif