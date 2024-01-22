import sys
from controls import DataArchiveService
from performance_decorator import timing_decorator

def show_help_and_dog():
    help_message = (
        "Commands:\n"
        "-all : Print all games list\n"
        "-count [SPORT] : Print games count for a specific sport\n"
        "-repr [SPORT] [FRAME COUNT] [FIXTURES COUNT] : Print representative data for a sport\n"
    )
    print(help_message)
    return "dog"


@timing_decorator
def run(file_name: str, args):
    archive_service = DataArchiveService(file_name)

    if '-all' in args:
        print(archive_service.games_list)
    elif '-count' in args:
        sport_index = args.index('-count') + 1
        if sport_index < len(args):
            sport = args[sport_index]
            print(archive_service.get_games_count_per_sport(sport.capitalize()))
        else:
            return show_help_and_dog()
    elif '-repr' in args:
        repr_index = args.index('-repr') + 1
        if repr_index + 2 < len(args):
            sport, start, end = args[repr_index:repr_index + 3]
            print(archive_service.get_representative_data(sport.capitalize(), int(start), int(end)))
        else:
            return show_help_and_dog()
    else:
        return show_help_and_dog()


if __name__ == '__main__':
    FILE_NAME = 'inventory_lsports-dev_full_14_03_2023_sample1M.parquet'
    run(FILE_NAME, sys.argv)