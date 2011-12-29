#include <stdio.h>
#include <string.h>

/* BUF_SIZEより長い行は扱わないものとする */
#define	BUF_SIZE	64

static char DELIM[] = " \t\n";

int main(int argc, char* argv[])
{
	char buf[BUF_SIZE];
	for (;;) {
		char *r = fgets(buf, sizeof(buf), stdin);
		if (r == NULL) break;

		char *tp;
		for(tp = strtok(buf, DELIM); tp; tp = strtok(NULL, DELIM)) {
			printf("%s\t1\n", tp);
		}
	}

	return 0;
}
