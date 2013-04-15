#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define POWER3(x) ((x)*(x)*(x))
#define POWER6(x) (POWER3(x)*POWER3(x))

#define DEBUG 0
#define DEBUG_IA 0
#define DEBUG_IB 6
#define DEBUG_IC DecodeToIC(0x15, 0x15)
int DEBUG2 = 0;
#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)

#define IA_ABC 0
#define IA_AAB 1
#define IA_ABA 2
#define IA_ABB 3
#define IA_AAA 4
char* alist[] = {"Aabc", "Aaab", "Aaba", "Aabb", "Aaaa" };
int aoffset[] = {0, 64, 64 + 27, 64 + 2 * 27, 64 + 81};
int alimit[]  = {64, 27, 27, 27, 8};

int b_shift[5][3] = {
		{16, 4, 1},
		{9, 3, 1},
		{9, 3, 1},
		{9, 3, 1},
		{4, 2, 1}
	};
// char sign[3] = {0, 1, -1};
short power3[] = {1, 3, 9, 27, 81, 243, /* not used */ 729};

int power3x[4][6] ={
	 {0, 0, 0, 0, 0, 0},
	 {1, 3, 9, 27, 81, 243},
	 {2, 6, 18, 54, 162, 486},
	 {2, 6, 18, 54, 162, 486}
	};
/* abc: 1 * 4**3
 * aab/aba/abb: 3 * 3**3
 * aaa: 1 * 2**3
 * in all 64 + 81 + 8 = 153
 */

static inline int IAB(int ia, int ib) {
#if 0
	if (ia > 5) {
		fprintf(stderr, "ia > 5\n");
		exit(1);
	}
	if (ib > alimit[ia]) {
		fprintf(stderr, "ib > alimit\n");
		exit(1);
	}
#endif
	return aoffset[ia] + ib;
}

struct Index {
	/* range from 0 to 4**3 */
	char right2;
	char bottom2;
} idx[153][POWER6(3)];

static inline Index* GetIndex(int ia, int ib, int ic) {
	return &idx[IAB(ia, ib)][ic]; 
}
static inline char& GetRight(int ia, int ib, int ic) {
	return idx[IAB(ia, ib)][ic].right2;
}
static inline char& GetBottom(int ia, int ib, int ic) {
	return idx[IAB(ia, ib)][ic].bottom2;
}
static inline char EncodeRightOrBottom(int a0, int a1, int a2, int a3) {
	int diff1 = a1 - a0;
	int diff2 = a2 - a1;
	int diff3 = a3 - a2;
	return 	((diff1 & 0x3) << 4) |
		((diff2 & 0x3) << 2) |
		((diff3 & 0x3) << 0);
}
static inline int DecodeToIC(int left, int top) {
	// code 0, 1, 3
	// IC   1, 2, 0
	// sign 0, 1, -1
	int diff4 = ((left) + 1) & 0x3;
	left >>= 2;
	int diff2 = ((left) + 1) & 0x3;
	left >>= 2;
	int diff0 = ((left) + 1) & 0x3;

	int diff5 = ((top) + 1) & 0x3;
	top >>= 2;
	int diff3 = ((top) + 1) & 0x3;
	top >>= 2;
	int diff1 = ((top) + 1) & 0x3;
	int ret;
	ret = 	power3x[diff0][0] + 
		power3x[diff1][1] +
		power3x[diff2][2] +
		power3x[diff3][3] +
		power3x[diff4][4] +
		power3x[diff5][5];
	//vcvg fprintf(stderr, "decode 0x%x 0x%x to %d (%d/%d/%d/%d/%d/%d)\n", left, top, ret, diff0, diff1, diff2, diff3, diff4, diff5);
	return ret;
}
static inline char DecodeToSum(int left, int top) {
	static const char sign[] = {0, 1, 0, -1};
	return 	sign[left >> 4] +
		sign[(left >> 2) & 0x3] +
		sign[left & 0x3] +

		sign[top >> 4] +
		sign[(top >> 2) & 0x3] +
		sign[top & 0x3];
}
static inline char DecodeToSumB2(int bottom) {
	static const char sign[] = {0, 1, 0, -1};
	return 	sign[(bottom >> 2) & 0x3] +
		sign[bottom & 0x3];
}

static inline char DecodeToSumB1(int bottom) {
	static const char sign[] = {0, 1, 0, -1};
	return 	sign[bottom & 0x3];
}
static inline void DecodeDPLeftAndTopFromIC(int dp[4][4], int ic) {
	static const char sign[] = {-1, 0, 1};
	dp[0][0] = 0;
	int c = ic;
	for (int i = 1; i < 4; i ++) {
		dp[i][0] = dp[i - 1][0] + sign[c % 3];
		c /= 3;
		dp[0][i] = dp[0][i - 1] + sign[c % 3];
		c /= 3;
	}
}


struct IndexUsage {
	int corner;
	/* range from 0 to 3**6 */
	char left2;
	char top2;
};

#define VERIFY 0

#if 0
static int decode_right_bottom(int left, int bottom) {
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
#endif
#if 0
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
#endif
#if 0
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
#endif

#if 0
static int encode_right(int dp[4][4]) {
	return __encode(dp[0][3], dp[1][3], dp[2][3], dp[3][3], 0);
}

static int encode_bottom(int dp[4][4]) {
	return __encode(dp[3][0], dp[3][1], dp[3][2], dp[3][3], 1);
}
#endif
static inline int encode_ia(int a0, int a1, int a2) {
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


static inline int encode_ia_from_half(int a012) {
	return encode_ia(a012 >> 10, (a012 >> 5) & 31, a012 & 31);
}

static inline int encode_b(int ia, int a0, int a1, int a2, int b) {
	if (b == a0)
		return 0;
	switch (ia) {
	case 0: // abc
		if (b == a1)
			return 1;
		if (b == a2)
			return 2;
		return 3;
	case 1:
		if (b == a2)
			return 1;
		return 2;
	case 2:
	case 3:
		if (b == a1)
			return 1;
		return 2;
	case 4:
		return 1;
	default:
		fprintf(stderr, "ia > 5\n");
		exit(1);
		return -1;
	}
}

static inline int encode_ib(int ia, int a0, int a1, int a2, int b0, int b1, int b2) {
	return encode_b(ia, a0, a1, a2, b0) * b_shift[ia][0] +
		encode_b(ia, a0, a1, a2, b1) * b_shift[ia][1] +
		encode_b(ia, a0, a1, a2, b2);
}

int BTABLE[4][4][4] = {
	{ {0, 1, 2, -1}, {3, 4, 5, -1}, {6, 7, 8, -1}, {-1, -1, -1, -1} },
	{ {9, 10, 11, -1}, {12, 13, 14, -1}, {15, 16, 17, -1}, {-1, -1, -1, -1} },
	{ {18, 19, 20, -1}, {21, 22, 23, -1}, {24, 25, 26, -1}, {-1, -1, -1, -1} },
	{ {-1, -1, -1, -1}, {-1, -1, -1, -1}, {-1, -1, -1, -1}, {-1, -1, -1, -1} },
};
static inline int encode_b_abc(int a0, int a1, int a2, int b) {
	if (unlikely(b == a0))
		return 0;
	if (unlikely(b == a1))
		return 1;
	return (2 | likely(b != a2));
}
static inline int encode_b_ab(int a0, int a1, int b) {
	if (unlikely(b == a1))
		return 1;
	return (likely(b != a0) << 1);
}
static inline int encode_b_a(int a0, int b) {
	return likely(b != a0);
}
int FASTTABLE[5] = {
	(0 << 4) | (1 << 2) | 2,
	(0 * 9) + (0 * 3) + 1,
	(0 * 9) + (1 * 3) + 0,
	(0 * 9) + (1 * 3) + 1,
	(0 << 2) | (0 << 2) | 0
	
};
static inline int encode_ib_from_half(int ia, int a012, int b012) {
	if (a012 == b012) {
		return FASTTABLE[ia];
	}
	int a0, a1, a2;
	int b0, b1, b2;
	int B0, B1, B2;
	b0 = b012 >> 10;
	b1 = (b012 >> 5) & 31;
	b2 = b012 & 31;
	switch(ia) {
	case 0: // abc
		a0 = a012 >> 10;
		a1 = (a012 >> 5) & 31;
		a2 = a012 & 31;
		return
		(encode_b_abc(a0, a1, a2, b0) << 4) |
		(encode_b_abc(a0, a1, a2, b1) << 2) |
		(encode_b_abc(a0, a1, a2, b2));
	case 1:	// aab
		a0 = a012 >> 10;
		a1 = a012 & 31;
		goto HANDLE123;
	case 2: // aba
		a1 = (a012 >> 5) & 31;
		a0 = a012 & 31;
		goto HANDLE123;
	case 3: // abb
		a0 = a012 >> 10;
		a1 = a012 & 31;
HANDLE123:
		B0 = encode_b_ab(a0, a1, b0); // value [0,3)
		B1 = encode_b_ab(a0, a1, b1);
		B2 = encode_b_ab(a0, a1, b2);
		return BTABLE[B0][B1][B2];
	case 4: 
		a0 = a012 & 31;
		return
		(encode_b_a(a0, b0) << 2) |
		(encode_b_a(a0, b1) << 1) |
		(encode_b_a(a0, b2));
	default:
		return -1;
	}
}
static inline int encode_ib_from_half1(int ia, int a012, int b012) {
	return encode_ib(ia, a012 >> 10, (a012 >> 5) & 31, a012 & 31, b012 >> 10, (b012 >> 5) & 31, b012 & 31);
}


static int build_index() {
	char b[5] = {'B', 0, 0, 0, 0};
	int total_counter = 0;
	int state_counter = 0;
#if DEBUG
	for (int ia = DEBUG_IA; ia < DEBUG_IA + 1; ia++) {
#else
	for (int ia = 0; ia < 5; ia++) {
#endif
		char* a = alist[ia];
#if DEBUG
		for (int ic = DEBUG_IC; ic < DEBUG_IC + 1; ic ++) {
#else
		for (int ic = 0; ic < POWER6(3); ic ++) {
#endif
			int dp[4][4];
			// decode_left_top(dp, ic);
			DecodeDPLeftAndTopFromIC(dp, ic);
#if VERIFY
			int left = ic / power3[0] % 3 * power3[0] +
			           ic / power3[2] % 3 * power3[2] +
			           ic / power3[4] % 3 * power3[4];
			int top  = ic / power3[1] % 3 * power3[1] +
			           ic / power3[3] % 3 * power3[3] +
			           ic / power3[5] % 3 * power3[5];
#endif

#if DEBUG
			for (int ib_search = DEBUG_IB; ib_search < DEBUG_IB + 1; ib_search ++) {
#else
			for (int ib_search = 0; ib_search < 64; ib_search ++) {
#endif
				b[1] = ((ib_search >> 4) & 0x03) + 'a';
				b[2] = ((ib_search >> 2) & 0x03) + 'a';
				b[3] = ((ib_search >> 0) & 0x03) + 'a';

				int ib = encode_ib(ia, a[1], a[2], a[3], b[1], b[2], b[3]);

				for (int i = 1; i < 4; i++) {
					for (int j = 1; j < 4; j++) {
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
				GetRight(ia, ib, ic) = EncodeRightOrBottom(dp[0][3], dp[1][3], dp[2][3], dp[3][3]);
				GetBottom(ia, ib, ic) = EncodeRightOrBottom(dp[3][0], dp[3][1], dp[3][2], dp[3][3]);

#if DEBUG || VERIFY
#if DEBUG
				if (DEBUG) {
#elif VERIFY
				int left_bottom, right_top;
				if ( (left_bottom = DecodeToSum(left, GetBottom(ia, ib, ic))) !=
					(right_top = DecodeToSum(GetRight(ia, ib, ic), top))) {
					if (left_bottom != right_top)
						fprintf(stderr, "    ERROR!! left-bottom %d != top-right %d\n", left_bottom, right_top);
#endif

					fprintf(stdout, "ia/ic/ib = %d/%d/%d\n", ia, ic, ib);
					fprintf(stdout, "    %3c %3c %3c %3c\n", b[0], b[1], b[2], b[3]);
					for (int i = 0; i < 4; i++) {
						fprintf(stdout, "%3c %3d %3d %3d %3d\n", a[i], dp[i][0], dp[i][1], dp[i][2], dp[i][3]);
					}
					fprintf(stdout, "right/bottom = 0x%x/0x%x\n", GetRight(ia, ib, ic), GetBottom(ia, ib, ic));
					fprintf(stdout, "(maybe it does not exists?)\n");
#if VERIFY
					exit(1);
#endif
				}
#endif
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
static inline int edit_distance(const char* _a, const char* _b) {
	return edit_distance(_a, _b, 0);
}
int init_code;
void fastindex_init() {
	init_code = EncodeRightOrBottom(0, 1, 2, 3);
}
static inline int edit_distance_h(const unsigned short* _a, int la, const unsigned short* _b, int lb) {
	const unsigned short* a = _a;
	const unsigned short* b = _b;
	struct IndexUsage dp[(la-1)/3 + 2][16];

	for (int i = 0; i < (la-1)/3+2; i ++) {
		dp[i][0].corner = i * 3;
		dp[i][0].left2 = init_code;
	}
	for (int i = 0; i < (lb-1)/3+2; i ++) {
		dp[0][i].corner = i * 3;
		dp[0][i].top2 = init_code;
	}

	for (int i = 0; i < la; i += 3) {
		for (int j = 0; j < lb; j += 3) {
			int ia = encode_ia_from_half(a[i / 3]);
			int ib = encode_ib_from_half(ia, a[i / 3], b[j / 3]);
			int left = dp[i/3][j/3].left2;
			int top = dp[i/3][j/3].top2;
			int ic = DecodeToIC(left, top);
			Index* p = GetIndex(ia, ib, ic);
			dp[i/3][j/3 + 1].left2 = p->right2;
			dp[i/3 + 1][j/3].top2 = p->bottom2;
			int corner = dp[i/3][j/3].corner + DecodeToSum(left, p->bottom2);
			dp[i/3 + 1][j/3 + 1].corner = corner;
		}
	}
	int last = dp[(la - 1)/3 + 1][(lb - 1)/3 + 1].corner;
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
			last -= DecodeToSumB2(dp[i / 3 + 1][j / 3].top2);
			break;
		case B1:
			last -= DecodeToSumB1(dp[i / 3 + 1][j / 3].top2);
			break;
		case R2:
			last -= DecodeToSumB2(dp[i / 3][j / 3 + 1].left2);
			break;
		case R1:
			last -= DecodeToSumB1(dp[i / 3][j / 3 + 1].left2);
			break;

	}
	return last;
}
#if 0 || 1
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

	int init_code  = EncodeRightOrBottom(0, 1, 2, 3);

	struct IndexUsage dp[11][11];

	for (int i = 0; i < 11; i ++) {
		dp[0][i].corner = i * 3;
		dp[0][i].top2 = init_code;
		dp[i][0].corner = i * 3;
		dp[i][0].left2 = init_code;
	}

	int last = 0;

	for (int i = 0; i < la; i += 3) {
		for (int j = 0; j < lb; j += 3) {
			int ia = encode_ia(a[i], a[i + 1], a[i + 2]);
			int ib = encode_ib(ia, a[i], a[i + 1], a[i + 2], b[j + 0], b[j + 1], b[j + 2]);
			int left = dp[i/3][j/3].left2;
			int top = dp[i/3][j/3].top2;
			// struct Index* p = &idx[IAB(ia, ib)][DecodeToIC(left, top)];
			if (DEBUG2) {
				fprintf(stderr, "i, j = %d, %d\n", i, j);
				fprintf(stderr, "    ia, ib = %d, %x\n", ia, ib);
				fprintf(stderr, "    lefttop = %d\n", dp[i/3][j/3].corner);
				//vcvg fprintf(stderr, "    top = %d, (%s)\n", top, debug_bottom(top));
				//vcvg fprintf(stderr, "    left = %d, (%s)\n", left, debug_left(left));
				//vcvg fprintf(stderr, "    p->right = %d, (%s)\n", p->right, debug_left(p->right));
				//vcvg fprintf(stderr, "    p->bottom = %d, (%s)\n", p->bottom, debug_bottom(p->bottom));
			}
			int ic = DecodeToIC(left, top);
#if VERIFY
			int bottom = GetBottom(ia, ib, ic);
			int right = GetRight(ia, ib, ic);
			if (DecodeToSum(left, bottom) !=
				DecodeToSum(right, top)) {
				fprintf(stderr, "    ERROR!! left-bottom != top-right\n");
			}
#endif
			dp[i/3][j/3 + 1].left2 = GetRight(ia, ib, ic);
			dp[i/3 + 1][j/3].top2 = GetBottom(ia, ib, ic);
			int corner = dp[i/3][j/3].corner + DecodeToSum(left, GetBottom(ia, ib, DecodeToIC(left, top)));
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
			last -= DecodeToSumB2(dp[i / 3 + 1][j / 3].top2);
			break;
		case B1:
			last -= DecodeToSumB1(dp[i / 3 + 1][j / 3].top2);
			break;
		case R2:
			last -= DecodeToSumB2(dp[i / 3][j / 3 + 1].left2);
			break;
		case R1:
			last -= DecodeToSumB1(dp[i / 3][j / 3 + 1].left2);
			break;

	}
	if (DEBUG2) {
		fprintf(stderr, "%s %s %d\n", _a, _b, last);
		fprintf(stderr, "\n");
	}
	return last;
}
#endif

static void MY_ASSERT(int x, int y) {
	if (x != y) {
		fprintf(stderr, "my assert %d != result %d\n", x, y);
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
	fastindex_init();
	build_index();
}

int FastEditDistanceH(const unsigned short* a, int na, const unsigned short* b, int nb) {
	return edit_distance_h(a, na, b, nb);
}

int FastEditDistance(const char* a, int na, const char* b, int nb) {
	return edit_distance(a, b, 0);
}
