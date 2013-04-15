#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define POWER3(x) ((x)*(x)*(x))
#define POWER6(x) (POWER3(x)*POWER3(x))

#define DEBUG 0
#define DEBUG_IA 3
#define DEBUG_IB 0xf
#define DEBUG_IC (273 + 91)
int DEBUG2 = 0;
struct Index {
	int right;
	int bottom;
} idx[5][POWER6(3)][64];

struct IndexUsage {
	int left;
	int top;
	int corner;
};

char sign[3] = {0, 1, -1};
int power3[] = {1, 3, 9, 27, 81, 81 * 3, 81 * 9, 81 * 27};
int decode_right_bottom(int left, int bottom) {
	return sign[left / power3[0] % 3] +
		sign[left / power3[2] % 3] +
		sign[left / power3[4] % 3] +
		sign[bottom / power3[1] % 3] +
		sign[bottom / power3[3] % 3] +
		sign[bottom / power3[5] % 3];
}
static char* debug_left(int left) {
	static __thread char dbg[128];
	snprintf(dbg, 128, "%d %d %d", sign[left / power3[0] % 3], sign[left / power3[2] % 3], sign[left / power3[4] % 3]);
	return dbg;
}
static char* debug_bottom(int bottom) {
	static __thread char dbg[128];
	snprintf(dbg, 128, "%d %d %d", sign[bottom / power3[1] % 3], sign[bottom / power3[3] % 3], sign[bottom / power3[5] % 3]);
	return dbg;
}
static int decode_left_top(int dp[4][4], int ic) {
	/* ic can be written in base-3 numeral system.
		c_5 c_4 c_3 c_2 c_1 c_0   (0 <= c_i < 3)

	   And then the top and left are decoded as:
			 0  c_1 c_3 c_5
			c_0
			c_2
			c_4
	*/

	dp[0][0] = 0;
	int c = ic;
	for (int i = 1; i < 4; i ++) {
		dp[i][0] = dp[i - 1][0] + sign[c % 3];
		c /= 3;
		dp[0][i] = dp[0][i - 1] + sign[c % 3];
		c /= 3;
	}
	return 0;
}
static int encode_sign(int sign) {
	/* or (3 + sign) % 3 */
	if (sign == -1)
		return 2;
	else
		return sign;
}

static int __encode(int a0, int a1, int a2, int a3, int is_bottom) {
	int diff[] = {0, encode_sign(a1 - a0), encode_sign(a2 - a1), encode_sign(a3 - a2)};
	int ret;
	if (! is_bottom) {
		ret =  diff[1] * power3[0] +
			diff[2] * power3[2] +
			diff[3] * power3[4];
	}
	else {
		ret =  diff[1] * power3[1] +
			diff[2] * power3[3] +
			diff[3] * power3[5];
	}
#if 0
	fprintf(stderr, "encoding a0123=%d,%d,%d,%d ==> diff %d,%d,%d,%d, %d\n",
			a0, a1, a2, a3, diff[0], diff[1], diff[2], diff[3], ret);
#endif
	return ret;
}

static int encode_right(int dp[4][4]) {
	return __encode(dp[0][3], dp[1][3], dp[2][3], dp[3][3], 0);
}

static int encode_bottom(int dp[4][4]) {
	return __encode(dp[3][0], dp[3][1], dp[3][2], dp[3][3], 1);
}

static int encode_ia(int a0, int a1, int a2) {
	if (a0 == a1) {
		// 1 or 4
		if (a0 == a2)
			return 4;
		else
			return 1;
	}
	else {
		if (a0 == a2)
			return 2;
		if (a1 == a2)
			return 3;
		return 0;
	}
}

static int encode_b(int ia, int a0, int a1, int a2, int b) {
	if (b == a0)
		return 0;
	switch (ia) {
	case 0: // abc
		if (b == a1)
			return 1;
		if (b == a2)
			return 2;
		break;
	case 1:
		if (b == a2)
			return 1;
		break;
	case 2:
	case 3:
		if (b == a1)
			return 1;
		break;
	case 4:
		break;
	}
	return 3;
}

static int encode_ib(int ia, int a0, int a1, int a2, int b0, int b1, int b2) {
	return encode_b(ia, a0, a1, a2, b0) << 4 |
		encode_b(ia, a0, a1, a2, b1) << 2 |
		encode_b(ia, a0, a1, a2, b2);
}

static int build_index() {
	int ia;
	int ib;
	int ic;
	int i;
	int j;
	char* alist[] = {"Aabc", "Aaab", "Aaba", "Aabb", "Aaaa" };
	char b[5] = {'B', 0, 0, 0, 0};
	int total_counter = 0;
	int state_counter = 0;
#if DEBUG
	for (ia = DEBUG_IA; ia < DEBUG_IA + 1; ia++) {
#else
	for (ia = 0; ia < 5; ia++) {
#endif
		char* a = alist[ia];
#if DEBUG
		for (ic = DEBUG_IC; ic < DEBUG_IC + 1; ic ++) {
#else
		for (ic = 0; ic < POWER6(3); ic ++) {
#endif
			int dp[4][4];
			decode_left_top(dp, ic);
			int left = ic / power3[0] % 3 * power3[0] +
			           ic / power3[2] % 3 * power3[2] +
			           ic / power3[4] % 3 * power3[4];
			int top  = ic / power3[1] % 3 * power3[1] +
			           ic / power3[3] % 3 * power3[3] +
			           ic / power3[5] % 3 * power3[5];

#if DEBUG
			for (ib = DEBUG_IB; ib < DEBUG_IB + 1; ib ++) {
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
						if (a[i] == b[j]) {
							if (dp[i-1][j-1] < best)
								best = dp[i-1][j-1];
						}
						else {
							if (dp[i-1][j-1] + 1 < best)
								best = dp[i-1][j-1] + 1;
						}
						dp[i][j] = best;
						total_counter += 1;
					}
				}

				state_counter += 1;
				struct Index* p = &idx[ia][ic][ib];
				idx[ia][ic][ib].right = encode_right(dp);
				idx[ia][ic][ib].bottom = encode_bottom(dp);

				int left_bottom = 0;
				int right_top = 0;
				if (DEBUG || (
					(left_bottom = decode_right_bottom(left, p->bottom)) !=
					(right_top = decode_right_bottom(p->right, top)))) {
					if (left_bottom != right_top)
						fprintf(stderr, "    ERROR!! left-bottom %d != top-right %d\n", left_bottom, right_top);

					fprintf(stdout, "ia/ic/ib = %d/%d/%d\n", ia, ic, ib);
					fprintf(stdout, "    %3c %3c %3c %3c\n", b[0], b[1], b[2], b[3]);
					for (i = 0; i < 4; i++) {
						fprintf(stdout, "%3c %3d %3d %3d %3d\n", a[i], dp[i][0], dp[i][1], dp[i][2], dp[i][3]);
					}
					fprintf(stdout, "right/bottom = %d/%d\n", idx[ia][ic][ib].right, idx[ia][ic][ib].bottom);
					fprintf(stdout, "\n");
					exit(1);
				}
			}
		}
	}
#if DEBUG
	fprintf(stdout, "total %d\n", total_counter);
	fprintf(stdout, "state %d\n", state_counter);
#endif
	return 0;
}

static int edit_distance(const char* _a, const char* _b, int trailing_zeros);
static int edit_distance(const char* _a, const char* _b) {
	return edit_distance(_a, _b, 0);
}
static int edit_distance(const char* _a, const char* _b, int trailing_zeros) {
	const char* a = _a;
	const char* b = _b;
	char a_arr[32];
	char b_arr[32];
	if (trailing_zeros) {
		a = _a;
		b = _b;
	}
	else {
		a = a_arr;
		b = b_arr;
		memset(a_arr, 0, sizeof(a_arr));
		memset(b_arr, 0, sizeof(b_arr));
		strcpy(a_arr, _a);
		strcpy(b_arr, _b);
	}

	int la = strlen(a);
	int lb = strlen(b);

	int start_top = __encode(0, 1, 2, 3, 1);
	int start_left = __encode(0, 1, 2, 3, 0);

	struct IndexUsage dp[11][11];

	for (int i = 0; i < 11; i ++) {
		dp[0][i].corner = i * 3;
		dp[0][i].top = start_top;
		dp[i][0].corner = i * 3;
		dp[i][0].left = start_left;
	}

	int last = 0;

	for (int i = 0; i < la; i += 3) {
		for (int j = 0; j < lb; j += 3) {
			int ia = encode_ia(a[i], a[i + 1], a[i + 2]);
			int ib = encode_ib(ia, a[i], a[i + 1], a[i + 2], b[j + 0], b[j + 1], b[j + 2]);
			int left = dp[i/3][j/3].left;
			int top = dp[i/3][j/3].top;
			struct Index* p = &idx[ia][top + left][ib];
			if (DEBUG2) {
				fprintf(stderr, "i, j = %d, %d\n", i, j);
				fprintf(stderr, "    ia, ib = %d, %x\n", ia, ib);
				fprintf(stderr, "    lefttop = %d\n", dp[i/3][j/3].corner);
				fprintf(stderr, "    top = %d, (%s)\n", top, debug_bottom(top));
				fprintf(stderr, "    left = %d, (%s)\n", left, debug_left(left));
				fprintf(stderr, "    p->right = %d, (%s)\n", p->right, debug_left(p->right));
				fprintf(stderr, "    p->bottom = %d, (%s)\n", p->bottom, debug_bottom(p->bottom));
			}
			if (decode_right_bottom(left, p->bottom) !=
				decode_right_bottom(p->right, top)) {
				fprintf(stderr, "    ERROR!! left-bottom != top-right\n");
			}
			dp[i/3][j/3 + 1].left = p->right;
			dp[i/3 + 1][j/3].top = p->bottom;
			int corner = dp[i/3][j/3].corner + decode_right_bottom(left, p->bottom);
			if (DEBUG2) {
				fprintf(stderr, "    corner = %d\n", corner);
			}
			dp[i/3 + 1][j/3 + 1].corner = corner;
			last = corner;
		}
	}
	if (DEBUG2) {
		fprintf(stderr, "%s %s %d\n", _a, _b, last);
	}
	/*

				(lb%3)	0 |	1	2	0
			(la%3)
			0		0 |	B2	B1	0
			------------------------------------------
			1		R2|	0	R1	R2
			2		R1|	B1	0	R1
			0		0 |	B2	B1	0

	*/
	const int B1 = 1, B2 = 2, R1 = 3, R2 = 4;
	int TAB[3][3] = {
			{0,  B2, B1},
			{R2,  0, R1},
			{R1, B1,  0}};
	int i = (la - 1);
	int j = (lb - 1);
	switch(TAB[la % 3][lb % 3]) {
		case B2:
			last -= sign[dp[i / 3 + 1][j / 3].top / power3[3] % 3];
		case B1:
			last -= sign[dp[i / 3 + 1][j / 3].top / power3[5] % 3];
			break;
		case R2:
			last -= sign[dp[i / 3][j / 3 + 1].left / power3[2] % 3];
		case R1:
			last -= sign[dp[i / 3][j / 3 + 1].left / power3[4] % 3];
			break;

	}
	if (DEBUG2) {
		fprintf(stderr, "%s %s %d\n", _a, _b, last);
		fprintf(stderr, "\n");
	}
	return last;
}

static void MY_ASSERT(int x, int y) {
	if (x != y) {
		fprintf(stderr, "x != y\n");
		exit(1);
	}
}
#if 1
int fastindex_main() {
	build_index();
	DEBUG2 = 1;
	MY_ASSERT(0, edit_distance("abc", "abc"));
	MY_ASSERT(1, edit_distance("abc", "abcd"));
	MY_ASSERT(2, edit_distance("abc", "abcde"));
	MY_ASSERT(3, edit_distance("abc", "abcdef"));
	MY_ASSERT(4, edit_distance("abc", "abcdefg"));
	MY_ASSERT(5, edit_distance("abc", "abcdefgh"));
	MY_ASSERT(6, edit_distance("abc", "abcdefghi"));
	MY_ASSERT(7, edit_distance("abc", "abcdefghij"));
	MY_ASSERT(1, edit_distance("abcd", "abc"));
	MY_ASSERT(0, edit_distance("abcd", "abcd"));
	MY_ASSERT(1, edit_distance("abcd", "abcde"));
	MY_ASSERT(2, edit_distance("abcd", "abcdef"));
	MY_ASSERT(3, edit_distance("abcd", "abcdefg"));
	MY_ASSERT(4, edit_distance("abcd", "abcdefgh"));
	MY_ASSERT(5, edit_distance("abcd", "abcdefghi"));
	MY_ASSERT(6, edit_distance("abcd", "abcdefghij"));
	MY_ASSERT(2, edit_distance("abcde", "abc"));
	MY_ASSERT(1, edit_distance("abcde", "abcd"));
	MY_ASSERT(0, edit_distance("abcde", "abcde"));
	MY_ASSERT(1, edit_distance("abcde", "abcdef"));
	MY_ASSERT(2, edit_distance("abcde", "abcdefg"));
	MY_ASSERT(3, edit_distance("abcde", "abcdefgh"));
	MY_ASSERT(4, edit_distance("abcde", "abcdefghi"));
	MY_ASSERT(5, edit_distance("abcde", "abcdefghij"));

	MY_ASSERT(2, edit_distance("abc", "cba"));
	MY_ASSERT(6, edit_distance("dreams", "anti"));
	MY_ASSERT(9, edit_distance("aephtipte", "behavioural"));
	
	return 0;
}
#endif

void FastEditDistanceBuildIndex() {
	build_index();
}

int FastEditDistance(const char* a, int na, const char* b, int nb) {
	return edit_distance(a, b, 0);
}
