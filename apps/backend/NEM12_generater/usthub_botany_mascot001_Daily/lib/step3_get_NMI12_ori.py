"""
Generate NMI12 original(no duplicated 300 N)

100 fixed format
...
200 fixed format
300 interval difference, quality flag(whole day missing: N, partial missing: V, no missing: A)
400 miss time interval(if 300 flag-V, then 400 must be many rows, 1 to 96 intervals. Else no 400 rows)
610 total consumption in current day, total consumption with index reading.
710 fixed format
...
910 fixed format

for missing data:
if all day missing -> estimate_missing_days()
partial missing -> interpolate_missing_values()

There will be an extreme case here:
There are 97 time points throughout the day, and no data is received at two consecutive time points.
This will create a very confusing phenomenon:
300 rows are V
but there is no 400 rows, because they are all F
(The format of 1, 96, F should not appear)
"""


from datetime import datetime, timedelta
import pandas as pd
import logging
import os
from decimal import Decimal, getcontext, ROUND_HALF_UP


logging.basicConfig(
    filename='log/daily_monitor.log',
    level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set the global precision, assuming the maximum allowed number length is 15 digits
getcontext().prec = 15


# Create a dictionary corresponding to the required attributes of the mapping table
# old: {'9EMTA619025269':{HWMETERNMI:'W4WR01V001', Offset_usage:189125}
# now we set as: {'W4WR01V001':{HWMETERNO:'9EMTA619025269', Offset_usage:189125}
def fetch_nmi_mapping(mapping_file):
    """
    Load mappings from CSV files, including Offset_usage
    mapping_file: Water_Serial_Mapping.csv
    """
    mapping_df = pd.read_csv(mapping_file)

    # Filter out records where HWMETERNMI is 'NIL'
    mapping_df = mapping_df[mapping_df["HWMETERNMI"] != "NIL"]

    # Creating a mapping dictionary
    nmi_mapping = mapping_df.set_index("HWMETERNMI")[["HWMETERNO", "Offset_usage"]].to_dict("index")
    return nmi_mapping


# For days with missing data for the entire day, or days with missing data for multiple consecutive days,
# fill in the data based on the valid data from the two days before and after.
def estimate_missing_days(meter_data_by_day):
    """
    Interpolate missing data for one or more days and return two flags:
    - full_day_filled：Indicates which days are interpolated throughout the day.
    - equal_values_flag(now useless)：Indicates whether the maximum and minimum values
                        of the previous and next interpolation segments are equal.
    """
    full_day_filled = {}
    equal_values_flag = {}

    for nmi, daily_data in meter_data_by_day.items():
        days = sorted(daily_data.keys())  # Sort all days by time
        # Mark the status of the current water meter's daily data
        status = {day: all(v == '#' for v in daily_data[day]) for day in days}

        full_day_filled[nmi] = {}  # Initialize the flag of each water meter
        equal_values_flag[nmi] = {}

        missing_segments = []  # Store the start and end days of consecutive missing segments (start_day, end_day)
        start_day = None

        for day in days:
            if status[day]:  # The current day is completely missing
                if start_day is None:
                    start_day = day  # Start a new missing segment
            else:
                if start_day is not None:
                    # End the current missing segment
                    missing_segments.append((start_day, day))  # Save start and end points
                    start_day = None

        # If there are any missing days that have not been completed, fill them in
        if start_day is not None:
            missing_segments.append((start_day, days[-1] + timedelta(days=1)))

        # Interpolate each missing segment
        for start_day, end_day in missing_segments:
            # Find the nearest day with data on the left
            # Find the nearest day with data on the right
            prev_day = max([d for d in days if d < start_day and not status[d]], default=None)
            next_day = min([d for d in days if d > end_day and not status[d]], default=None)

            if prev_day and next_day:  # When there is data at both ends, perform integer interpolation
                prev_values = daily_data[prev_day]
                next_values = daily_data[next_day]
                max_value_prev = max(int(v) for v in prev_values if v != '#')
                min_value_next = min(int(v) for v in next_values if v != '#')
                missing_days = (end_day - start_day).days

                daily_step = (min_value_next - max_value_prev) // (missing_days + 1)

                # Perform overall interpolation for all missing days
                for i, missing_day in enumerate(pd.date_range(start=start_day, end=end_day - timedelta(days=1))):
                    # Use linear growth to assign values to each day's data points
                    # day_value = max_value_prev + (i + 1) * daily_step
                    day_value = max_value_prev + i * daily_step
                    daily_values = [None] * 97
                    daily_values[0] = day_value
                    for j in range(1, len(prev_values)):
                        daily_values[j] = daily_values[0] + (j * daily_step) // 96
                    # daily_values = [day_value + j * ((daily_step * 10) // 97) for j in range(len(prev_values))]
                    daily_data[missing_day] = daily_values

                    full_day_filled[nmi][missing_day] = True  # Mark as full day interpolation
                    # Set the equal_values_flag flag
                    equal_values_flag[nmi][start_day] = (max_value_prev == min_value_next)

            # When there is data only on the right, fill with the minimum value on the right
            elif not prev_day and next_day:
                next_values = daily_data[next_day]
                min_value_next = min(int(v) for v in next_values if v != '#')
                for missing_day in pd.date_range(start=start_day, end=end_day - timedelta(days=1)):
                    daily_values = [min_value_next] * len(daily_data[next_day])
                    daily_data[missing_day] = daily_values
                    full_day_filled[nmi][missing_day] = True
                    equal_values_flag[nmi][missing_day] = True

            # When there is data only on the left, fill with the maximum value on the left
            elif prev_day and not next_day:
                prev_values = daily_data[prev_day]
                max_value_prev = max(int(v) for v in prev_values if v != '#')
                for missing_day in pd.date_range(start=start_day, end=end_day - timedelta(days=1)):
                    daily_values = [max_value_prev] * len(daily_data[prev_day])
                    daily_data[missing_day] = daily_values
                    full_day_filled[nmi][missing_day] = True
                    equal_values_flag[nmi][missing_day] = True

    return meter_data_by_day, full_day_filled, equal_values_flag


def calculate_differences(values):
    """
    The unit of measurement is KL.
    Calculate the growth value(difference) of every two adjacent time points and divide it by 100,
    retaining 4 decimal places
    """
    differences = []
    for i in range(1, len(values)):
        if values[i] != '#' and values[i - 1] != '#':
            difference = Decimal(values[i] - values[i - 1]) / Decimal(100)
            # Keep 4 decimal places and round up
            difference = difference.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
            differences.append(difference)
        else:
            differences.append('#')  # If one of the adjacent time points is '#', the difference is '#'
    return differences


def create_numeric_15_3(value):
    """
    fixed format
    """
    # Create a Decimal object and round it to 3 decimal places.
    decimal_value = Decimal(value).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)

    if len(str(decimal_value).replace('.', '').replace('-', '')) > 15:
        logging.error("Value exceeds Numeric(15, 3) limits")
        raise ValueError("Value exceeds Numeric(15, 3) limits")

    return decimal_value


# Core function for estimate the missing value
# For days that are not completely missing, interpolate
def interpolate_missing_values(values, time_slots, prev_values=None, next_values=None):
    # values represents the data of the day [150, '#', '#', 160, ...]
    n = len(values)  # The data points for the day should be 97 (including 0:00 of the next day)
    interpolated = []
    filled_times = []

    # if all(v == '#' for v in values):
    #     print('There is still data with all # !!!!!!!!!!')
    #     print(values)

    # Handle the case of leading '#', fill all leading '#' with the first non-'#' value
    if values[0] == '#':
        first_non_hash = next((v for v in values if v != '#'), None)
        if first_non_hash is not None:
            interpolated = True
            for i in range(n):
                if values[i] == '#':
                    values[i] = int(first_non_hash)
                    filled_times.append(time_slots[i])
                else:
                    break

    # Traverse the array, find all '#' values that need to be interpolated,
    # and use the previous and next non-'#' values for integer interpolation
    for i in range(n):
        if values[i] == '#':
            start = i - 1
            end = i + 1

            # Look to the right for the first non-'#' value, and terminate if not found
            while end < n and values[end] == '#':
                end += 1

            # If a value is found that is not preceded and followed by '#', then interpolation is performed
            if start >= 0 and end < n and values[start] != '#' and values[end] != '#':
                interpolated = True
                # Get the previous and next values
                start_value = int(values[start])
                end_value = int(values[end])

                # Calculate the gap size (number of missing values)
                gap = end - start - 1
                if gap > 0:
                    # Calculate integer deltas and try to distribute them evenly over the missing time points
                    delta = (end_value - start_value) // (gap + 1)

                    # Fill in the middle `#` values one by one
                    for j in range(1, gap + 1):
                        values[start + j] = start_value + j * delta
                        filled_times.append(time_slots[start + j])

            # If only the first non-`#` value is found, and the rest are all `#`
            elif end >= n and start >= 0 and values[start] != '#':
                interpolated = True
                start_value = int(values[start])

                # Fill all the following `#` values
                for j in range(start + 1, n):
                    values[j] = start_value
                    filled_times.append(time_slots[j])

    # Return like:
    # current day
    # values = [100, '#', '#', 200, ...]
    # interpolated = True
    # time_slots = ['00:00', '00:15', '00:30', '00:45', ..., '00:00']
    return values, interpolated, filled_times


def get_litter_factor_by_nmi(mapping_file, nmi):
    """
    Read the mapping CSV, find the row with HWMETERNMI == nmi,
    return litter_factor (as float).
    This function reads the file once and closes it immediately.
    """
    df = pd.read_csv(mapping_file)

    row = df.loc[df["HWMETERNMI"] == nmi]

    if row.empty:
        raise ValueError(f"NMI {nmi} not found in mapping file {mapping_file}")

    # return as float (ensure numeric)
    return float(row.iloc[0]["litter_factor"])


# Definition: Each water meter has a daily format of 100, 200, 300, 400, 610, 710, 900
def expand_and_format(df, current_day, mapping, time_slots, filled_timepoints, full_day_filled, equal_values_flag):
    """
    Expand each row of processed data into a six-row format
    and add extra information to rows 200, 300, 400, and 610.
    add 100, 900 fixed format
    """
    expanded_rows = []
    # offset_sum = Decimal(0)  # Used to accumulate offset_plus_max_reading in every 610 rows, now useless

    date_str = current_day.strftime('%Y%m%d')  # Convert the date to YYYYMMDD format
    creation_time_str = datetime.now().strftime('%Y%m%d%H%M%S')  # Current time, for example 202408150946
    creation_time_str_short = datetime.now().strftime('%Y%m%d%H%M')

    for index, row in df.iterrows():
        nmi = index  # Get the corresponding nmi
        meter_info = mapping.get(nmi, {})  # Get water meter mapping information (NEWHWMETERNO, Offset_usage)
        meter_serial = meter_info.get('HWMETERNO', "")
        offset_usage = meter_info.get('Offset_usage', 0) or 0  # Set a default value of 0 for offset_usage

        # Get the filling status flag of the water meter for the day
        is_full_day_filled = full_day_filled.get(nmi, {}).get(current_day, False)
        is_equal_values = equal_values_flag.get(nmi, {}).get(current_day, False)

        # The second line 200, insert 6 specific values in front
        expanded_rows.append([
                                 '200', nmi, 'W1', '', 'W1', '', meter_serial, 'KL', '15', ''
                             ] + [''] * (96 - 9))  # generate row '200'

        # Processing Differences
        values = row.tolist()
        differences = calculate_differences(values)  # Calculate the difference and keep 4 decimal places

        # Calculate the sum of the differences for the day
        sum_of_differences = sum(d for d in differences if d != '#')

        # Get the maximum value of reading for the day, the default value is 0
        # max_reading = max((v for v in values[:-1] if v != '#' and v is not None), default=0)
        max_reading = max((v for v in values if v != '#' and v is not None), default=0)

        # Get the current water meter filling time points
        filled_times = filled_timepoints.get(nmi, [])

        if is_full_day_filled:
            # if is_equal_values:
            #     status = "N"
            # else:
            #     status = "V"
            status = "N"
        else:
            # Determine the status of the data for the day based on the number of filled time points (A, V, N)
            # if len(filled_times) == len(time_slots) - 1:  # '#' was insert in 96 slots in one day
            #     status = "N"  # all day missing
            if len(filled_times) == 0:  # No time points are filled
                status = "A"  # The data is valid all day, without interpolation and completion
            else:
                status = "V"  # Some data are missing and some are interpolated to complete

        # The third line 300, fill in the difference data and status
        # In the 300 row, enter the difference data.
        expanded_rows.append(
            ['300', date_str] + differences + [f"{status}", '', '', f"{creation_time_str}", ''])

        # Generate 400 rows based on 300 status
        if status == "N":
            pass
            # When a whole day is missing, generate interpolated segments from 1-96 (now useless)
            # expanded_rows.append(['400', '1', '96', 'N'])

        elif status == "V":
            # When dealing with partial deletions, distinguish between segments A and F
            time_slot_indices = set()
            for filled_time in filled_times:
                if filled_time in time_slots:
                    time_index = time_slots.get_loc(filled_time)
                    if time_index == 0:
                        time_slot_indices.add(1)
                    if 0 < time_index < len(time_slots) - 1:
                        # Mark the two time periods before and after
                        time_slot_indices.add(time_index)  # Previous time period
                        time_slot_indices.add(time_index + 1)  # The next time period
                    if time_index == 96:
                        time_slot_indices.add(time_index)

            # Generates a collection of indices for all time periods of the day (from 1 to 96)
            all_time_indices = set(range(1, 97))
            # Calculate the missing time period index set
            no_missing_indices = all_time_indices - time_slot_indices

            # Initialize the merged time period list
            time_slot_indices = sorted(time_slot_indices)
            no_missing_indices = sorted(no_missing_indices)
            combined_time_slots = []

            # Merge time periods and generate F and A segments
            def merge_time_slots(indices, label):
                if indices:
                    start = indices[0]
                    end = start
                    for i in range(1, len(indices)):
                        if indices[i] == end + 1:
                            end = indices[i]  # If it is a continuous time period, update end
                        else:
                            combined_time_slots.append((start, end, label))  # Merge and mark
                            start = indices[i]
                            end = start

                    combined_time_slots.append((start, end, label))  # Last segment merge

            # Merge time segments: interpolated segments (F) and non-missing segments (A)
            merge_time_slots(time_slot_indices, "F")
            merge_time_slots(no_missing_indices, "A")
            combined_time_slots = sorted(combined_time_slots, key=lambda x: x[0])

            # Adjust the format of line 400
            # Added check: Remove segments with start and end both 96 and marked "A"
            combined_time_slots = [slot for slot in combined_time_slots if
                                   not (slot[0] == 96 and slot[1] == 96 and slot[2] == "A")]
            # Added check: if a segment with start of 0 is encountered, change start to 1 (now useless)
            combined_time_slots = [(1 if start == 0 else start, end, label) for start, end, label in
                                   combined_time_slots]

            # Added check: When there is only one segment, and it is "400,1,96,A", delete
            if len(combined_time_slots) == 1 and combined_time_slots[0] == (1, 96, "A"):
                # combined_time_slots = [(1, 96, "F")]
                combined_time_slots = []
            # Added check: When there is only one segment, and it is "400,1,96,F", delete
            if len(combined_time_slots) == 1 and combined_time_slots[0] == (1, 96, "F"):
                # combined_time_slots = [(1, 96, "F")]
                print(f"meter: {nmi, meter_serial}, date: {current_day.strftime('%Y-%m-%d')} delete 1,96,F")
                combined_time_slots = []

            # finally generate 400 row
            for start, end, label in combined_time_slots:
                expanded_rows.append(['400', str(start), str(end), label, ""])

        # 610, fill in NEWHWMETERNMI information, the sum of the 300 rows of differences,
        # and Offset_usage + max_reading, must careful for the litter factor
        litter_factor = get_litter_factor_by_nmi("Meter_Serial_Mapping.csv", nmi)
        max_reading_factor = (1 * litter_factor) / 1000
        offset_plus_max_reading = create_numeric_15_3((offset_usage / 1000 + int(max_reading) / max_reading_factor))

        # 累加每个610行中的offset_plus_max_reading
        # offset_sum += Decimal(offset_plus_max_reading)

        expanded_rows.append(
            ['610', nmi, meter_serial, date_str, 'W1', sum_of_differences, offset_plus_max_reading] + [
                ''] * (96 - 6))

        expanded_rows.append(
            ['710', nmi, meter_serial, date_str, 'W1', create_numeric_15_3(1), offset_plus_max_reading] + [
                ''] * (96 - 6))

    # Delete the line containing "1, 96, N" from the 400 lines.
    filtered_rows = [
        row for row in expanded_rows
        if not (row[0] == '400' and row[1] == '1' and row[2] == '96' and row[3] == 'N')
    ]

    # 100 and 900
    header_row = [['100', 'NEM12', creation_time_str_short, 'X4MDP', 'EVERGY'] + [''] * (96 - 4)]
    footer_row = [['900'] + [''] * 96]
    expanded_df = pd.concat([pd.DataFrame(header_row), pd.DataFrame(filtered_rows), pd.DataFrame(footer_row)],
                            ignore_index=True)

    # print(f"Date: {date_str}, Total offset_plus_max_reading: {offset_sum}")
    return expanded_df


def save_to_csv(expanded_df, output_filename):
    """
    Save the DataFrame to a CSV file, remove extra commas at the end of each line, and handle NaN values
    """

    # Convert NaN values in DataFrame to empty string
    expanded_df = expanded_df.fillna('')

    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        for row in expanded_df.itertuples(index=False, name=None):
            # Convert each line to a string and remove the empty elements at the end
            row = list(row)
            while row and row[-1] == '':
                row.pop()  # Remove the empty string at the end

            f.write(','.join(map(str, row)) + '\r\n')  # Specify CRLF line breaks
    logging.info(f"Saved the file {output_filename}")
    print(f"Saved the file {output_filename} in CSV format with extra commas and remove NaNs from the end of the lines")


# Generate water meter for each meter_serial
def generate_daily_tables(messages_file, nmi_mapping, start_time, end_time):
    """
    Generates daily timestamp table based on messages_file and meter_serial_mapping
    """
    output_folder = "data_merged/NMI12"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    messages_df = pd.read_csv(messages_file, parse_dates=["timestamp"])

    # Initialize water meter data
    all_nmis = set(nmi_mapping.keys())  # nmi
    meter_data_by_day = {nmi: {} for nmi in all_nmis}

    current_day = start_time
    file_index = 1  # Document No.

    while current_day <= end_time:
        print(current_day)
        next_day = current_day + timedelta(days=1)
        time_slots = pd.date_range(start=current_day, end=next_day, freq="15min")

        # Initialize the DataFrame for the current day
        df = pd.DataFrame(index=list(all_nmis), columns=time_slots)
        df.loc[:, :] = "#"  # All data are initialized to missing values

        # Filter data for the current day
        day_data = messages_df[
            (messages_df["timestamp"] >= current_day) & (messages_df["timestamp"] <= next_day)
        ]

        # Fill in the readings for the day
        for _, row in day_data.iterrows():
            # meter_serial = row["meter_serial"]
            nmi = row["nmi"]
            timestamp = row["timestamp"]
            reading = row["reading"]

            # If the meter nmi exists in the mapping table
            if nmi in all_nmis:
                nearest_time_slot = min(time_slots, key=lambda t: abs(t - timestamp))
                df.loc[nmi, nearest_time_slot] = reading

        for nmi in df.index:
            meter_data_by_day[nmi][current_day] = df.loc[nmi].tolist()
            # {
            #     "ZZZZ048298-W": {
            #         "2025-10-27": ["#", "#", "100.5", "#", ...]   ← 96 slot one day
            # },
            #     "ZZZZ048299-W": {
            #         "2025-10-27": ["#", "150.2", "#", "#", ...]
            # }
            # }

        # Update to next day
        current_day = next_day

    # Filling missing data for all day
    meter_data_by_day, full_day_filled, equal_values_flag = estimate_missing_days(meter_data_by_day)

    # Generate daily files
    for current_day, next_day in zip(pd.date_range(start=start_time, end=end_time - timedelta(days=1), freq="D"),
                                     pd.date_range(start=start_time + timedelta(days=1), end=end_time, freq="D")):
        time_slots = pd.date_range(start=current_day, end=next_day, freq="15min")
        df = pd.DataFrame(index=list(all_nmis), columns=time_slots)

        for nmi in all_nmis:
            df.loc[nmi] = meter_data_by_day[nmi].get(current_day, ["#"] * len(time_slots))

        # Interpolation and formatting
        filled_timepoints = {}
        for nmi in df.index:
            values, interpolated, filled_times = interpolate_missing_values(df.loc[nmi].values, time_slots)
            df.loc[nmi] = values
            filled_timepoints[nmi] = filled_times

        expanded_df = expand_and_format(
            df, current_day, nmi_mapping, time_slots, filled_timepoints, full_day_filled, equal_values_flag
        )
        date_str = current_day.strftime("%Y%m%d")
        unique_id = f"{date_str}{file_index:05d}"

        # csv_filename = f"NEM12#{unique_id}#X4MDP#Evergy.csv"
        csv_filename = os.path.join(output_folder, f"NEM12#{unique_id}#X4MDP#Evergy.csv")
        file_index += 1

        save_to_csv(expanded_df, csv_filename)


def step3_main():
    # Generation time range
    today = datetime.now()
    start_test_day = today - timedelta(days=2)
    end_test_day = today
    start_time = datetime(start_test_day.year, start_test_day.month, start_test_day.day, 0, 0, 0)
    end_time = datetime(end_test_day.year, end_test_day.month, end_test_day.day, 23, 59, 59)
    logging.info(f"NMI12 startTime:{start_time}, endTime:{end_time}")

    messages_file = "data_merged/processed_messages.csv"
    mapping_file = "Meter_Serial_Mapping.csv"

    # Loading the Mapping Table
    nmi_mapping = fetch_nmi_mapping(mapping_file)

    # Generate a table of daily timestamps and save it as a CSV file
    generate_daily_tables(messages_file, nmi_mapping, start_time, end_time)

    logging.info("Generated!")
    print("Generated!")


if __name__ == "__main__":
    step3_main()
