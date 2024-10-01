import time

from Utilities.Utils import read_excel_data
from Configuration import config


start_test_case = config.start_test_case
end_test_case = config.end_test_case
def run_main():
    import main  # Delayed import to avoid circular import
    for index in range(2, 4):
        main.main(index)
        time.sleep(2)

if __name__ == "__main__":
    run_main()

