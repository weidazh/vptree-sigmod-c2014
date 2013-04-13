#include <stdio.h>
#include "utils.h"

unsigned char BSS[MAX_THREADS][SIZE_BSS];
__thread long offset = 0;
