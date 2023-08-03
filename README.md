# Enem_2020_2021_anomalies Data Modeling Project with PySpark

In this project, we leverage the capabilities of PySpark to prepare models for estimating "Natural Sciences" scores and "Color and Race" variables. The dataset we're working with is the Enem_2020_2021_anomalies database, which can be downloaded from the following link: [Enem_2020_2021_anomalies database](link_to_dataset).

![PySpark Logo](pyspark_logo.png)

## Project Tasks

1. **Evaluate the quality of the database**: Utilize PySpark's data manipulation capabilities to explore and understand the data types, detect possible outliers, identify missing values, and find inconsistent entries.

2. **Data preparation**: Correct variable types and handle other anomalies in the dataset using PySpark's robust data transformation functions.

3. **Building Models**: Impute missing values, handle outliers, and manage disparate values using PySpark's Machine Learning library (MLlib).

4. **Model development**: Develop three models for each variable - "Natural Sciences" scores and "Color and Race". This will result in a total of six (6) models. PySpark MLlib offers various algorithms for model development.

5. **Model evaluation**: Compare the models' performance for each variable to identify the best one. PySpark MLlib also provides tools for model evaluation.

6. **Model application**: Apply the best models to predict the variables using the ENEM_Score_Ciencia_Natureza and ENEM_Score_Cor_Raca files.

7. **Report development**: Develop a comprehensive report detailing your project, including the steps taken, challenges encountered, and results obtained.

8. **Presentation preparation**: Prepare a presentation summarizing the project, including its key aspects. Ensure it does not exceed 15 minutes.

## File Structure

The repository contains the following files and directories:

- `p1_CLEANING_AND_EXPLORING_enem_dataset.ipynb`: Jupyter Notebook for data cleaning and exploration.
- `p2_ML_MODEL_to_predict_the_natural_sciences_grade.ipynb`: Jupyter Notebook for developing the "Natural Sciences" grade prediction model.
- `p3_PREDICTING_the_natural_sciences_grade.ipynb`: Jupyter Notebook for predicting "Natural Sciences" grades using the developed model.
- `p4_ML_MODEL_to_predict_the_color_and_race.ipynb`: Jupyter Notebook for developing the "Color and Race" prediction model.
- `p5_PREDICTING_color_and_race.ipynb`: Jupyter Notebook for predicting "Color and Race" using the developed model.
- `requirements.txt`: List of Python packages required for the project.

Please follow the steps outlined in the notebooks to execute the project successfully. If you encounter any issues or have questions, feel free to reach out.

---

Note: The dataset link is not provided in the project description. 
