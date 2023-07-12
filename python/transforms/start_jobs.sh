#!/bin/bash
python periodic_impulse.py \
    --interval 3 \
    --total_seconds 11
    # No windowing applied.
python periodic_impulse.py \
    --interval 3 \
    --total_seconds 11 \
    --apply_windowing