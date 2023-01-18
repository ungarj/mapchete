import numpy as np


def execute(mp, color=(255, 1, 1)):
    return np.stack([np.full(mp.tile.shape, c) for c in color])
