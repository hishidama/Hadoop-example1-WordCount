#include <stdio.h>
#include <string.h>

/* BUF_SIZEより長い行は扱わないものとする */
#define	BUF_SIZE	64

static char DELIM[] = "\t\n";

static void output(char *old, int sum)
{
	if (sum != 0) {
		printf("%s\t%d\n", old, sum);
	}
}

int main(int argc, char* argv[])
{
	char buf[BUF_SIZE];
	char old[BUF_SIZE] = "";
	int sum = 0;
	for (;;) {
		char *r = fgets(buf, sizeof(buf), stdin);
		if (r == NULL) {
			output(old, sum);
			break;
		}

		char *word = strtok(buf, DELIM);
		if (strcmp(word, old) != 0) {
			output(old, sum);
			strcpy(old, word);
			sum = 0;
		}
		int count  = atoi(strtok(NULL, DELIM));
		sum += count;
	}

	return 0;
}
