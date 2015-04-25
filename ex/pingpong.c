#include <assert.h>
#include <netdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/poll.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>		/* superset of previous */
#include <netinet/tcp.h>

#define USE_UDP 0
#define BUSY_WAIT 0

#if USE_UDP
#define SOCK_TYPE SOCK_DGRAM
#else
#define SOCK_TYPE SOCK_STREAM
#endif

typedef struct announce_struct
{
  int s;
  char hostname[100];
  int port;			/* port */
} announce_struct, * announce_struct_t;

void die(char * f)
{
  perror(f);
  exit(1);
}

double current_time()
{
  struct timeval tp[1];
  gettimeofday(tp, 0);
  return tp->tv_sec * 1000000.0 + tp->tv_usec;
}

announce_struct_t announce()	/* socket */
{
  announce_struct_t a 
    = (announce_struct_t)malloc(sizeof(announce_struct));
  int s;
  if (0 != gethostname(a->hostname, sizeof(a->hostname))) 
    die("gethostname");
  a->hostname[sizeof(a->hostname)-1] = 0;
  s = socket(PF_INET, SOCK_TYPE, 0);
  if (-1 == s) die("socket");
  {
    struct sockaddr_in addr[1];
    addr->sin_family = AF_INET;
    addr->sin_port = 0;
    addr->sin_addr.s_addr = INADDR_ANY;
    if (0 != bind(s, (struct sockaddr*)addr, 
		  sizeof(struct sockaddr_in))) die("bind");
  }
  if (USE_UDP == 0) {
    if (-1 == listen(s, 1)) die("listen");
  }
  {
    struct sockaddr_in addr[1];
    int len = sizeof(addr);
    if (0 != getsockname(s, (struct sockaddr*)addr, &len))
      die("getsockname");
    a->port = (int)ntohs(addr->sin_port);
    fprintf(stdout, "%s %d\n", a->hostname, a->port);
    fflush(stdout);
  }
  a->s = s;
  return a;
}

announce_struct_t get_announce(announce_struct_t me)
{
  announce_struct_t a 
    = (announce_struct_t)malloc(sizeof(announce_struct) * 2);
  int i;
  for (i = 0; i < 2; i++) {
    char buf[100];
    if (0 == fgets(buf, sizeof(buf), stdin)) die("fgets");
    if (2 != sscanf(buf, "%s %d\n", a[i].hostname, &a[i].port))
      die("fscanf");
  }
  if (strcmp(a[0].hostname, a[1].hostname) > 0) {
    announce_struct tmp = a[0];
    a[0] = a[1];
    a[1] = tmp;
  }
  return a;
}

double * ping(int s, int n)
{
  char m[1];
  int i;
  int _ = fprintf(stderr, "%d bytes for record\n", 
		  sizeof(double) * n);
  double * rec = (double *)calloc(sizeof(double), n);
  struct pollfd fds[1];
  double roundtrip = 0.0;
  double start_t, end_t;
  fds->fd = s;
  fds->events = POLLIN;
  start_t = current_time();
  for (i = 0; i < n; i++) {
    double t0, t1;
    m[0] = 'a' + (i % 26);
    t0 = current_time();
    if (1 != send(s, m, 1, 0)) die("send");
#if BUSY_WAIT
    while (0 == poll(fds, 1, 0.0)) ;
#endif
    if (1 != recv(s, m, 1, 0)) die("recv");
    t1 = current_time();
    rec[i] = t1 - t0;
    roundtrip += (t1 - t0);
  }
  end_t = current_time();
  {
    double dt = end_t - start_t;
    fprintf(stderr, 
	    "wallclock time = %.1f us\n"
	    "total roundtrip = %.1f us\n"
	    "avg roundtrip = %.3f\n"
	    "running = %.1f%%\n",
	    dt, roundtrip, roundtrip / n,
	    (dt - roundtrip) / dt * 100.0);
  }
  return rec;
}

void pong(int s, int n)
{
  char m[1];
  int i;
  struct pollfd fds[1];
  fds->fd = s;
  fds->events = POLLIN;
  for (i = 0; i < n; i++) {
#if BUSY_WAIT
    while (0 == poll(fds, 1, 0.0)) ;
#endif
    if (1 != recv(s, m, 1, 0)) die("send");
    assert(m[0] == 'a' + i % 26);
    if (1 != send(s, m, 1, 0)) die("recv");
  }
}

int main()
{
  announce_struct_t me = announce();
  announce_struct_t sv = get_announce(me);
  int n = 10000;
  int s = -1;
  int i;
  for (i = 0; i < 2; i++) {
    if (strcmp(me->hostname, sv[i].hostname) != 0) break;
  }
  assert(i == 0 || i == 1);
  if (USE_UDP || i == 1) {
    struct sockaddr_in addr[1];
    struct hostent * h;
    if (USE_UDP) s = me->s;
    else {
      close(s);
      s = socket(PF_INET, SOCK_TYPE, 0);
    }
    h = gethostbyname(sv[i].hostname);
    if (h == 0) die("gethostbyname");
    addr->sin_family = AF_INET;
    addr->sin_port = htons((short)sv[i].port);
    addr->sin_addr.s_addr = *((u_int32_t*)(h->h_addr_list[0]));
    if (-1 == connect(s, (struct sockaddr*)addr, sizeof(addr))) 
      die("connect");
  } else {
    struct sockaddr_in addr[1];
    int one = 1;
    int len = sizeof(addr);
    int new_s = accept(me->s, (struct sockaddr*)addr, &len);
    if (-1 == new_s) die("accept");
    if (-1 == setsockopt(new_s, SOL_TCP, TCP_NODELAY, 
			 &one, sizeof(one))) die("setsockopt");
    close(me->s);
    s = new_s;
  }
  if (i == 0) {
    double * rec = ping(s, n);
    char file[1000];
    FILE * fp;
    sprintf(file, "/home/taue/proj/gxp3/%s_%s.dat",
	    sv[0].hostname, sv[1].hostname);
    fp = fopen(file, "wb");
    if (fp == 0) die("fopen");
    for (i = 1; i < n; i++) {
      fprintf(fp, "%f\n", rec[i]);
    }
    fclose(fp);
    fprintf(stderr, "record written to %s\n", file);
  } else {
    pong(s, n);
  }
  return 0;
}
