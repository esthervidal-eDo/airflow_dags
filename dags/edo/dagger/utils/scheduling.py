from datetime import datetime, timedelta
from numpy.random import randint, choice
import re

possible_minutes = [0, 30]

random_hours = {
    "early_morning": (0, 3),
    "mid_morning": (3, 6),
    "late_morning": (6, 9),
    "morning": (0, 9),
    "working_day": (9, 18),
    "early_night": (18, 21),
    "late_night": (21, 24),
    "night": (18, 24)
}
specific_weekdays = {
    "everyday": "*",
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
random_weekdays = {
    "day_of_week": (0, 7),
    "weekday": (0, 5),
    "weekend": (5, 7),
}

random_hour_crontab = re.compile("random_({weekparts})_({dayparts})".format(
    weekparts="|".join(specific_weekdays.keys()),
    dayparts="|".join(random_hours.keys())
))

random_day_of_week_crontab = re.compile("random_({weekparts})".format(
    weekparts="|".join(random_weekdays.keys())
))

airflow_crontab = re.compile(
    "(@once|@hourly|@daily|@weekly|@monthly|@yearly)")


def is_regular_crontab(s):
    crontab_regex = "\*(?:\/{n})?|{n}(?:-{n})?(?:\/{n})?(?:,{n}(?:-{n})?(?:\/{n})?)*"
    s_parts = re.split(" +", s)
    if len(s_parts) != 5:
        return False
    are_s_parts_regular_crontabs = [bool(_s) for _s in (
        re.compile(crontab_regex.format(n="[0-5]?\d")).match(s_parts[0]),
        re.compile(crontab_regex.format(n="[01]?\d|2[0-3]")).match(s_parts[1]),
        re.compile(crontab_regex.format(n="0?[1-9]|[12]\d|3[01]")).match(s_parts[2]),
        re.compile(crontab_regex.format(n="0?[1-9]|1[012]")).match(s_parts[3]),
        re.compile(crontab_regex.format(n="[0-6](\-[0-6])?")).match(s_parts[4])
    )]
    return all(are_s_parts_regular_crontabs)


def random_hour_bounded(day_part_string, week_part_string):
    hour_part = choice(possible_minutes)
    day_part = randint(*random_hours.get(day_part_string, (0, 23)))
    week_part = specific_weekdays.get(week_part_string, "*")
    return "{hour_part} {day_part} {week_part} * *".format(
        hour_part=hour_part, day_part=day_part, week_part=week_part)


def random_day_of_week_bounded(week_part_string):
    week_part = random_weekdays.get(week_part_string, (0, 6))
    return "0 6 {} * *".format(randint(*week_part))


def schedule(crontab_schedule):
    # SCHEDULES DEFINED SPECIFICALLY TO BE RANDOM
    if crontab_schedule is None:
        return None
    elif random_day_of_week_crontab.match(crontab_schedule):
        week_part = random_day_of_week_crontab.match(crontab_schedule).groups()
        return random_day_of_week_bounded(week_part)
    elif airflow_crontab.match(crontab_schedule):
        return crontab_schedule
    elif is_regular_crontab(crontab_schedule):
        return crontab_schedule
    elif random_hour_crontab.match(crontab_schedule):
        week_part, day_part = random_hour_crontab.match(crontab_schedule).groups()
        return random_hour_bounded(day_part, week_part)
    else:
        raise Exception("Unrecognized crontab expression", crontab_schedule)
