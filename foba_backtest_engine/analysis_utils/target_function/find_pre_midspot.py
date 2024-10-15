from numba import guvectorize
import numpy as np

@guvectorize(['void(float64[:], float64[:], float64[:], float64[:])'], '(n),(m),(m)->(n)', nopython=True, target='parallel')
def find_pre_midspots(tradeTime, eventTime, midSpots, matched_midspots):
    n = tradeTime.shape[0]
    m = eventTime.shape[0]
    j = 0  # Initialize the eventTime index tracker
    for i in range(n):
        matched_midspots[i] = np.nan  # Initialize with NaN indicating no match
        while j < m and eventTime[j] < tradeTime[i]:
            j += 1
        if j > 0 and eventTime[j-1] < tradeTime[i]:
            matched_midspots[i] = midSpots[j-1]