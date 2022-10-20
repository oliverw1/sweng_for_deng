# Software engineering for data workers

📚 A course brought to you by the [Data Minded Academy].

## Context

These are the exercises used in the course *Software Engineering for data 
engineers*, developed by instructors at Data Minded. The exercises are meant
to be completed in the lexicographical order determined by name of their
parent folders. That is, exercises inside the folder `b_foo` should be
completed before those in `c_bar`, but both should come after those of
`a_foo_bar`.

## Getting started

While you can clone the repo locally, we do not offer support for setting up
your coding environment. Instead, we recommend you [tackle the exercises
using Gitpod][this gitpod].

[![Open in Gitpod][gitpod logo]][this gitpod]


⚠ IMPORTANT: Create a new branch and periodically push your work to the remote.
After 30min of inactivity this environment shuts down and you will lose unsaved
progress.

# Course objectives

- Introduce good software engineering practices.
- Illustrate modular and easily testable data transformation pipelines using
  PySpark.
- Illustrate tools helping you to achieve CI/CD.

# Intended audience

- People with working experience with the Python programming language.
  Knowledge of basics of PySpark is ideal, though not required.
- Familiar with Python functions, variables and the container data types of
  `list`, `tuple`, `dict`, and `set`.

# Approach

There is a high degree of participation expected from the students: they
will need to write code themselves and reason on topics, so that they can
better retain the knowledge. 
  
Participants are recommended to be working on a branch for any changes they
make, to avoid conflicts (otherwise the onus is on the participant), as the
instructors may choose to release an update to the current branch.

Note: this course is not about writing the best pipelines possible. There are
many ways to skin a cat, in this course we show one (or sometimes a few), which
should be suitable for the level of the participants.

## Exercises

### Common business case 1: cleaning data

Using the information you've learned about clean coding practices, prepare a
sizeable dataset for storage in "the clean zone" of a data lake, by
implementing the `clean` function of
[clean_flights_starter.py](exercises/h_cleansers/clean_flights_starter.py).

### Warm-up: thinking critically about tests

Glance at the file
[distance_metrics.py](./exercises/b_unit_test_demo/distance_metrics.py). Then,
complete [test_distance_metrics.py](./tests/test_distance_metrics.py), by
writing at least two useful tests, one of which should prove that the code, as
it is, is wrong.

### Adding derived columns

With that implemented, it's time to take a step back and think about how one
would compare data that might be distributed over different machines. Implement
`assert_frames_functionally_equivalent` from
[comparers.py](tests/comparers.py), a module containing helpful utility
functions fo testing purposes.  Verify that your implementation is correct by
running the test suite at [test_comparers.oy](tests/test_comparers.py). You
will use this function in a few subsequent exercises.

Return to [dates.py](exercises/c_labellers/dates.py) and
implement `label_weekend`. Again, run the related test from
[test_labellers.py](tests/test_labellers.py). It might be more useful to you if
you first read the test.


[this gitpod]: https://gitpod.io/#https://github.com/oliverw1/sweng_for_deng
[gitpod logo]: https://gitpod.io/button/open-in-gitpod.svg
[Data Minded Academy]: https://www.dataminded.academy/
