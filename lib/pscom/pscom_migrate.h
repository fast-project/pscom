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

#ifndef _PSCOM_MIGRATE_H_
#define _PSCOM_MIGRATE_H_
#include "pscom_priv.h"

#include <signal.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>

#ifdef MOSQUITTO_ENABLED
#include <mosquitto.h>
#endif

int pscom_migration_init(void);
int pscom_migration_cleanup(void);

int pscom_suspend_non_migratable_plugins(void);
int pscom_resume_non_migratable_plugins(void);

void pscom_migration_handle_shutdown_req(void);
void pscom_migration_handle_resume_req(void);

#endif /* _PSCOM_MIGRATE_H_ */
