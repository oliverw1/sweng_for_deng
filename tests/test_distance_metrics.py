"""
Exercise: implement a series of tests with which you validate the
correctness (or lack thereof) of the function great_circle_distance.
"""

from exercises.b_unit_test_demo.distance_metrics import great_circle_distance


def test_great_circle_distance():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    # Case 1: Null distance; same input values
    expected_distance = 0
    output_distance = great_circle_distance(0,0,0,0)
    assert output_distance == expected_distance

def test_great_circle_distance_neg_values():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    # Case 1: Null distance; same input values
    expected_distance = 0
    output_distance = great_circle_distance(-10,-10,-10,-10)
    assert output_distance == expected_distance

def test_great_circle_distance_distinct_values():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    # Case 1: Null distance; same input values
    expected_distance = 157.2
    output_distance = great_circle_distance(0,0,1,1)
    assert output_distance == expected_distance

def test_great_circle_distance_same_distance():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    # Case 1: Null distance; same input values
    expected_distance = 157.2
    output_distance_1 = great_circle_distance(0,0,1,1)
    output_distance_2 = great_circle_distance(1,1,2,2)
    assert output_distance_1 == output_distance_2
