"""
Exercise: implement a series of tests with which you validate the
correctness (or lack thereof) of the function great_circle_distance.
"""

from exercises.b_unit_test_demo.distance_metrics import great_circle_distance
import geopy
from geopy import distance


def test_great_circle_distance():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    assert great_circle_distance(0,0,0,0) == 0
    assert great_circle_distance(10,0,10,0) == 0

    # instead of thinking about 1000 scenarios and hope one of them would be false it's better to take a step back and make sure we are using the right formulas / compare with available  libraries -__-    
    assert distance.distance((10,0),(20,0)).km == great_circle_distance(10,0,20,0)


    pass
