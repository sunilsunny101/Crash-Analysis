Crash Analysis

This repository delves into a comprehensive analysis of U.S. accident data, providing valuable insights into patterns and trends.

Project Structure

For optimal organization and clarity, the project adheres to a well-defined directory structure:

data (folder): Holds all the raw data essential for the crash analysis, ensuring easy access and reproducibility.

output (folder): Houses the final analysis output, currently presented as a Google Colab notebook. Choosing this format facilitates interactive exploration for those familiar with the platform. Due to the relatively small size of the resulting dataframes, this approach proves efficient.

transformations (folder): Embraces the core analysis code. Within this directory:

    Functions_pyspark.py: Houses meticulously crafted functions specifically designed for the crash analysis.
    code.py: Imports and leverages the functions defined in Functions_pyspark.py, taking responsibility for the data loading process. This segregation promotes code clarity and modularity.
