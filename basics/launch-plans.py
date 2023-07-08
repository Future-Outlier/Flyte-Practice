import calendar

import datetime

from flytekit import LaunchPlan, current_context, task, workflow


@task
def square(val: int) -> int:
    return val * val


@workflow
def my_wf(val: int) -> int:
    result = square(val=val)
    return result


default_lp = LaunchPlan.get_default_launch_plan(current_context(), my_wf)
square_3 = default_lp(val=3)

my_lp = LaunchPlan.create("default_4_lp", my_wf, default_inputs={"val": 4})
square_4 = my_lp()
square_5 = my_lp(val=5)

my_fixed_lp = LaunchPlan.get_or_create(name="always_2_lp", workflow=my_wf, fixed_inputs={"val": 2})
square_2 = my_fixed_lp()
# error:
# square_1 = my_fixed_lp(val=1)

@task
def greet(day_of_week: str, number: int, am: bool) -> str:
    greeting = "Have a great " + day_of_week + " "
    greeting += "morning" if am else "evening"
    return greeting + "!" * number


@workflow
def go_greet(day_of_week: str, number: int, am: bool = False) -> str:
    return greet(day_of_week=day_of_week, number=number, am=am)


morning_greeting = LaunchPlan.create(
    "morning_greeting",
    go_greet,
    fixed_inputs={"am": True},
    default_inputs={"number": 1},
)

# Let's see if we can convincingly pass a Turing test!
today = datetime.datetime.today()
for n in range(7):
    day = today + datetime.timedelta(days=n)
    weekday = calendar.day_name[day.weekday()]
    if day.weekday() < 5:
        print(morning_greeting(day_of_week=weekday))
    else:
        # We're extra enthusiastic on weekends
        print(morning_greeting(number=3, day_of_week=weekday))


