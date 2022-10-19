"""
Exercise: implement a series of tests with which you validate the
correctness (or lack thereof) of the function great_circle_distance.
"""

from exercises.b_unit_test_demo.distance_metrics import great_circle_distance


def test_great_circle_distance():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    lat=50.893941
    lon=4.255856
    distance1 = great_circle_distance(latitude1=lat, longitude1=lon, latitude2=lat, longitude2=lon)
    print(f"the distance = {distance1}")
    
    lat_angola=-11.460008762078743
    lon_angola= 16.301662342883414
    distance = great_circle_distance(latitude1=lat, longitude1=lon, latitude2=lat_angola, longitude2=lon_angola)
    print(f"the distance = {distance}")
    
    lat_brazil =-38.11511953503118
    lon_brazil = -66.06931401694112
    distance = great_circle_distance(latitude1=lat_brazil, longitude1=lon_brazil, latitude2=lat_angola, longitude2=lon_angola)
    print(f"the distance = {distance}")
    assert distance1 == 0

def test_great_circle_distance0():
    lat=5
    lon=5
    lat2=6
    lon2=6
    distance1 = great_circle_distance(latitude1=lat, longitude1=lon, latitude2=lat2, longitude2=lon2)
    print(f"the distance = {distance1}")
    assert distance1 == 0

def test_great_circle_distance_check():
    pass
    
    