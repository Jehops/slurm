/*****************************************************************************\
 ** mpichgm.c - srun support for MPICH-GM (GMPI)
 ** $Id$
 *****************************************************************************
 *  Copyright (C) 2004 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Takao Hatazaki <takao.hatazaki@hp.com>
 *  UCRL-CODE-226842.
 *  
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://www.llnl.gov/linux/slurm/>.
 *  
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission 
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and 
 *  distribute linked combinations including the two. You must obey the GNU 
 *  General Public License in all respects for all of the code used other than 
 *  OpenSSL. If you modify file(s) with this exception, you may extend this 
 *  exception to your version of the file(s), but you are not obligated to do 
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in 
 *  the program, then also delete it here.
 *  
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *  
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#ifdef WITH_PTHREADS
#  include <pthread.h>
#endif

#include <signal.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <strings.h>

#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/net.h"
#include "src/common/mpi.h"

#include "src/plugins/mpi/mpichgm/mpichgm.h"

typedef struct {
	int defined;
	unsigned int port_board_id;
	unsigned int unique_high_id;
	unsigned int unique_low_id;
	unsigned int numanode;
	unsigned int remote_pid;
	unsigned int remote_port;
} gm_slave_t;

#define GMPI_RECV_BUF_LEN 65536


static int gmpi_fd = -1;

static int _gmpi_parse_init_recv_msg(slurm_mpi_jobstep_info_t *job, char *rbuf,
				     gm_slave_t *slave_data, int *ii)
{
	unsigned int magic, id, port_board_id, unique_high_id,
		unique_low_id, numanode, remote_pid, remote_port;
	int got;
	gm_slave_t *dp;

	got = sscanf(rbuf, "<<<%u:%u:%u:%u:%u:%u:%u::%u>>>",
		     &magic, &id, &port_board_id, &unique_high_id,
		     &unique_low_id, &numanode, &remote_pid, &remote_port);
	*ii = id;
	if (got != 8) {
		error("GMPI master received invalid init message");
		return -1;
	}
	if (magic != job->jobid) {
		error("GMPI master received invalid magic number");
		return -1;
	}
	if (id >= job->step_layout->task_cnt)
		fatal("GMPI id is out of range");
	if (port_board_id == 0)
		fatal("MPI id=%d was unable to open a GM port", id);

	dp = &slave_data[id];
	if (dp->defined) {
		error("Ignoring the message from MPI id=%d", id);
		return -1;
	}
	dp->defined = 1;
	dp->port_board_id = port_board_id;
	dp->unique_high_id = unique_high_id;
	dp->unique_low_id = unique_low_id;
	dp->numanode = numanode;
	dp->remote_pid = remote_pid;
	dp->remote_port = remote_port;

	debug3("slave_data[%d]: <<<%u:%u:%u:%u:%u:%u:%u::%u>>>",
	       id, magic, id, port_board_id,
	       dp->unique_high_id, dp->unique_low_id, dp->numanode,
	       dp->remote_pid, dp->remote_port);
	return 0;
}


static int _gmpi_establish_map(slurm_mpi_jobstep_info_t *job)
{
	struct sockaddr_in addr;
	in_addr_t *iaddrs;
	socklen_t addrlen;
	int accfd, newfd, rlen, nprocs, i, j, id;
	size_t gmaplen, lmaplen, maplen;
	char *p, *rbuf = NULL, *gmap = NULL, *lmap = NULL, *map = NULL;
	char tmp[128];
	gm_slave_t *slave_data = NULL, *dp;
	
	/*
	 * Collect info from slaves.
	 * Will never finish unless slaves are GMPI processes.
	 */
	accfd = gmpi_fd;
	addrlen = sizeof(addr);
	nprocs = job->step_layout->task_cnt;
	iaddrs = (in_addr_t *)xmalloc(sizeof(*iaddrs)*nprocs);
	slave_data = (gm_slave_t *)xmalloc(sizeof(*slave_data)*nprocs);
	for (i=0; i<nprocs; i++)
		slave_data[i].defined = 0;
	i = 0;
	rbuf = (char *)xmalloc(GMPI_RECV_BUF_LEN);
	
	while (i < nprocs) {
		newfd = accept(accfd, (struct sockaddr *)&addr, &addrlen);
		if (newfd == -1) {
			error("accept(2) in GMPI master thread: %m");
			continue;
		}
		rlen = recv(newfd, rbuf, GMPI_RECV_BUF_LEN, 0);
		if (rlen <= 0) {
			error("GMPI master recv returned %d", rlen);
			close(newfd);
			continue;
		} else {
			rbuf[rlen] = 0;
		}
		if (_gmpi_parse_init_recv_msg(job, rbuf, slave_data,
					      &id) == 0) {
			i++;
			iaddrs[id] = ntohl(addr.sin_addr.s_addr);
		}
		close(newfd);
	}
	xfree(rbuf);
	debug2("Received data from all of %d GMPI processes.", i);

	/*
	 * Compose the global map string.
	 */
	gmap = (char *)xmalloc(128*nprocs);
	p = gmap;
	strcpy(p, "[[[");
	p += 3;
	for (i=0; i<nprocs; i++) {
		dp = &slave_data[i];
		sprintf(tmp, "<%u:%u:%u:%u>", dp->port_board_id,
			dp->unique_high_id, dp->unique_low_id, dp->numanode);
		strcpy(p, tmp);
		p += strlen(tmp);
	}
	strcpy(p, "|||");
	p += 3;
	gmaplen = (size_t)(p - gmap);

	/*
	 * Respond to slaves.
	 */
	lmap = (char *)xmalloc(128*nprocs);
	for (i=0; i<nprocs; i++) {
		/*
		 * Compose the string to send.
		 */
		dp = &slave_data[i];
		p = lmap;
		for (j=0; j<nprocs; j++) {
			if (iaddrs[i] == iaddrs[j] &&
			    (dp->numanode == slave_data[j].numanode)) {
				sprintf(tmp, "<%u>", j);
				strcpy(p, tmp);
				p += strlen(tmp);
			}
		}
		lmaplen = (size_t)(p - lmap);
		map = (char *)xmalloc(gmaplen+lmaplen+4);
		strcpy(map, gmap);
		strcpy(map+gmaplen, lmap);
		strcpy(map+gmaplen+lmaplen, "]]]");
		maplen = gmaplen + lmaplen + 3;

		/*
		 * Send it.
		 */
		if ((newfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
			fatal("GMPI master failed to respond");
		}
		j = 1;
		if (setsockopt(newfd, SOL_SOCKET, SO_REUSEADDR,
			       (void *)&j, sizeof(j)))
			error("setsockopt in GMPI master: %m");
		bzero(&addr, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(iaddrs[i]);
		addr.sin_port = htons(dp->remote_port);
		if (connect(newfd, (struct sockaddr *)&addr, sizeof(addr)))
			fatal("GMPI master failed to connect");
		send(newfd, map, maplen, 0);
		close(newfd);
		xfree(map);
	}
	xfree(slave_data);
	xfree(lmap);
	xfree(gmap);
	xfree(iaddrs);

	debug2("GMPI master responded to all GMPI processes");
	return 0;
}


static void _gmpi_wait_abort(slurm_mpi_jobstep_info_t *job)
{
	struct sockaddr_in addr;
	socklen_t addrlen;
	int newfd, rlen;
	unsigned int magic;
	char *rbuf;

	rbuf = (char *)xmalloc(GMPI_RECV_BUF_LEN);
	addrlen = sizeof(addr);
	while (1) {
		newfd = accept(gmpi_fd, (struct sockaddr *)&addr,
			       &addrlen);
		if (newfd == -1) {
			fatal("GMPI master failed to accept (abort-wait)");
		}
		rlen = recv(newfd, rbuf, GMPI_RECV_BUF_LEN, 0);
		if (rlen <= 0) {
			error("GMPI recv (abort-wait) returned %d", rlen);
			close(newfd);
			continue;
		} else {
			rbuf[rlen] = 0;
		}
		if (sscanf(rbuf, "<<<ABORT_%u_ABORT>>>", &magic) != 1) {
			error("GMPI (abort-wait) received spurious message.");
			close(newfd);
			continue;
		}
		if (magic != job->jobid) {
			error("GMPI (abort-wait) received bad magic number.");
			close(newfd);
			continue;
		}
		close(newfd);
		debug("Received ABORT message from an MPI process.");
		slurm_signal_job_step(job->jobid, job->stepid, SIGKILL);
#if 0
		xfree(rbuf);
		close(jgmpi_fd);
		gmpi_fd = -1;
		return;
#endif
	}
}


static void *_gmpi_thr(void *arg)
{
	slurm_mpi_jobstep_info_t *job;

	job = (slurm_mpi_jobstep_info_t *) arg;

	debug3("GMPI master thread pid=%lu", (unsigned long) getpid());
	_gmpi_establish_map(job);
	
	debug3("GMPI master thread is waiting for ABORT message.");
	_gmpi_wait_abort(job);

	return (void *)0;
}


extern int gmpi_thr_create(slurm_mpi_jobstep_info_t *job, char ***env)
{
	short port;
	pthread_attr_t attr;
	pthread_t gtid;

	/*
	 * It is possible for one to modify the mpirun command in
	 * MPICH-GM distribution so that it calls srun, instead of
	 * rsh, for remote process invocations.  In that case, we
	 * should not override envs nor open the master port.
	 */
	if (getenv("GMPI_PORT"))
		return (0);

	if (net_stream_listen (&gmpi_fd, &port) < 0) {
		error ("Unable to create GMPI listen port: %m");
		return -1;
	}

	/*
	 * Accept in a separate thread.
	 */
	slurm_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	if (pthread_create(&gtid, &attr, &_gmpi_thr, (void *)job)) {
		slurm_attr_destroy(&attr);
		return -1;
	}
	slurm_attr_destroy(&attr);
	setenvf (NULL,               "GMPI_PORT",  "%u", port);
	env_array_overwrite_fmt(env, "GMPI_PORT",  "%u", port);
	setenvf (NULL,               "GMPI_MAGIC", "%u", job->jobid);
	env_array_overwrite_fmt(env, "GMPI_MAGIC", "%u", job->jobid);
	setenvf (NULL,               "GMPI_NP",    "%d",
		 job->step_layout->task_cnt);
	env_array_overwrite_fmt(env, "GMPI_NP",    "%d", 
				job->step_layout->task_cnt);
	setenvf (NULL,               "GMPI_SHMEM", "1");
	env_array_overwrite_fmt(env, "GMPI_SHMEM", "1");
	/* FIXME for multi-board config. */
	setenvf (NULL,               "GMPI_BOARD", "-1");
	env_array_overwrite_fmt(env, "GMPI_BOARD", "-1");

	debug("Started GMPI master thread (%lu)", (unsigned long) gtid);

	return 0;
}
