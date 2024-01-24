
# README.md for Sports Archive Project

## Project Overview
The Sports Archive Project is designed for efficient management and querying of a large sports game dataset. It allows users to perform various operations such as listing all games, counting games per sport, and retrieving specific representative data.

## Key Components
1. **Main Script (`archive.py`)**: 
   - Manages command-line arguments to perform operations like listing games, counting games per sport, and displaying representative data.
   - Integrates `DataArchiveService` for data operations and `timing_decorator` for performance tracking.

2. **Data Archive Service (`controls.py`)**:
   - Central service for data handling.
   - Reads and processes data from a Parquet file.
   - Offers functionalities like game listing, game count by sport, and representative data retrieval.

3. **Performance Decorator (`performance_decorator.py`)**:
   - A decorator for measuring and reporting function execution times.

4. **Unit Tests (`tests` directory)**:
   - Contains tests for `DataArchiveService`.
   - Ensures the accuracy and reliability of service functionalities.

## Installation
Before running the project, install the necessary dependencies:
   ```
   pip install -r requirements.txt
   ```

## How to Run
Place the Parquet data file in the project directory. Run `archive.py` with the following arguments for different functionalities:

1. **Counting Games by Sport**:
   ```
   python archive.py -count [SPORT]
   ```
   Replace `[SPORT]` with the desired sport name to count the number of games for that sport.

2. **Displaying Representative Data for a Sport**:
   ```
   python archive.py -repr [SPORT] [FRAME COUNT] [FIXTURES COUNT]
   ```
   Replace `[SPORT]`, `[FRAME COUNT]`, and `[FIXTURES COUNT]` with the desired sport name, frame count, and fixtures count, respectively. This command shows representative data for the specified sport.

3. **Testing date processing**:
   ```
   python archive.py -test
   ```
Example Commands:
- To count games for Tennis:
  ```
  python archive.py -count tennis
  ```
- To display representative data for Tennis:
  ```
  python archive.py -repr tennis 100 200
  ```
- Testing date processing:
  ```
  python archive.py -test
  ```

## Development Environment
- Python 3.x
- Pandas library
- PyArrow for Parquet file handling

## Testing
Execute the unit tests in the `tests` directory to check the functionalities. Passing all tests ensures system reliability.

## Future Enhancements
- Expand the range of sports data.
- Add advanced data analysis features.
- Optimize for larger datasets.

## Contributions
We welcome contributions via the standard fork-pull request workflow.



This README offers a basic guide to the Sports Archive Project. For comprehensive documentation, consult the inline comments and docstrings in the codebase.
