import datetime
base_string = "dateTime="
final_string = ""
time_delta = datetime.timedelta(minutes=15)
start_time = datetime.datetime(year=2020, month=8, day=28, hour=0, minute=0, second=0, microsecond=0)
minute_slots = list(range(0, 96))
for i in range(0,96):
    minute_slots[i] = minute_slots[i] * time_delta +start_time
    final_string = final_string + base_string + str(minute_slots[i].date()) + "T" + str(minute_slots[i].time()) + "Z&"
    print(final_string)

