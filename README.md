# Building solid data pipelines with PySpark

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

- Introduce good data engineering practices.
- Illustrate modular and easily testable data transformation pipelines using
  PySpark.
- Illustrate CI/CD tools

# Intended audience

- People working with (Py)Spark or soon to be working with it.
- Familiar with Python functions, variables and the container data types of
  `list`, `tuple`, `dict`, and `set`.

# Approach

Lecturer first sets the foundations right for Python development and
gradually builds up to PySpark data pipelines.

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

### Warm-up: thinking critically about tests

Glance at the file
[distance_metrics.py](./exercises/b_unit_test_demo/distance_metrics.py). Then,
complete [test_distance_metrics.py](./tests/test_distance_metrics.py), by
writing at least two useful tests, one of which should prove that the code, as
it is, is wrong.

### Adding derived columns

Check out [dates.py](exercises/c_labellers/dates.py) and implement the pure
python function `is_belgian_holiday`. verify your correct implementation by
running the test `test_pure_python_function` from
[test_labellers](tests/test_labellers.py). you could do this from the command
line with `pytest tests/test_labellers.py::test_pure_python_function`.

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

Finally, implement `label_holidays` from [dates](exercises/c_labellers/dates.py). 
As before, run the relevant test to verify a few easy cases (keep in mind that 
few tests are exhaustive: it's typically easier to prove something is wrong, 
than that something is right).

If you're making great speed, try to think of an alternative implementation 
to `label_holidays` and discuss pros and cons.

### (Optional) Get in the habit of writing test

Have a look at [date_helper.py](exercises/d_laziness/date_helper.py). Explain the intent of the
author. Which two key aspects to Spark's processing did the author forget? If 
you can't answer this, run `test_date_helper_doesnt_work_as_intended` from 
[test_laziness.py](exercises/d_laziness/test_laziness.py). Now write an alternative to the 
`convert_date` function that does do what the author intended.

### Common business case 1: cleaning data

Using the information seen in the videos, prepare a sizeable dataset for 
storage in "the clean zone" of a data lake, by implementing the `clean` 
function of [clean_flights_starter.py](exercises/h_cleansers/clean_flights_starter.py).

[this gitpod]: https://gitpod.io/#https://github.com/oliverw1/sweng_for_deng
[gitpod logo]: https://gitpod.io/button/open-in-gitpod.svg
[Data Minded Academy]: https://www.dataminded.academy/
