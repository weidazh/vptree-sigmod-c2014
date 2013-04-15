#include <stdio.h>
int main() {
	int ia;
	int ib;
	int ic;
	int i;
	int j;
	char* alist[] = {"Aabc", "Aaab", "Aaba", "Aabb", "Aaaa" };
	char b[5] = {'B', 0, 0, 0, 0};
	char sign[3] = {0, 1, -1};
	int total_counter = 0;
	int state_counter = 0;
#define DEBUG 0
#if DEBUG
	for (ia = 0; ia < 1; ia++) {
#else
	for (ia = 0; ia < 5; ia++) {
#endif
		char* a = alist[ia];
#define POWER3(x) ((x)*(x)*(x))
#define POWER6(x) (POWER3(x)*POWER3(x))
#if DEBUG
		for (ic = 273; ic < 273 + 1; ic ++) {
#else
		for (ic = 0; ic < POWER6(3); ic ++) {
#endif
			int dp[4][4];
			dp[0][0] = 0;
			int c = ic;
			for (i = 1; i < 4; i ++) {
				dp[i][0] = dp[i - 1][0] + sign[c % 3];
				c /= 3;
				dp[0][i] = dp[0][i - 1] + sign[c % 3];
				c /= 3;
			}
#if DEBUG
			for (ib = 6; ib < 6 + 1; ib ++) {
#else
			for (ib = 0; ib < 64; ib ++) {
#endif
				b[1] = ((ib >> 4) & 0x03) + 'a';
				b[2] = ((ib >> 2) & 0x03) + 'a';
				b[3] = ((ib >> 0) & 0x03) + 'a';

				for (i = 1; i < 4; i++) {
					for (j = 1; j < 4; j++) {
						int best = dp[i][j-1] + 1;
						if (dp[i-1][j] + 1 < best)
							best = dp[i-1][j] + 1;
						if (a[i] == b[j] && dp[i-1][j-1] < best) {
							best = dp[i-1][j-1];
						}
						dp[i][j] = best;
						total_counter += 1;
					}
				}

				state_counter += 1;
				fprintf(stdout, "ia/ic/ib = %d/%d/%d\n", ia, ic, ib);
				fprintf(stdout, "    %3c %3c %3c %3c\n", b[0], b[1], b[2], b[3]);
				for (i = 0; i < 4; i++) {
					fprintf(stdout, "%3c %3d %3d %3d %3d\n", a[i], dp[i][0], dp[i][1], dp[i][2], dp[i][3]);
				}
			}
		}
	}
	fprintf(stdout, "total %d\n", total_counter);
	fprintf(stdout, "state %d\n", state_counter);
	return 0;
}
