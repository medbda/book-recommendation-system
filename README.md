# Hybrid book-recommendation-system
Hybrid Book Recommendation System


Overview :
This project implements a Hybrid Book Recommendation System that combines Collaborative Filtering using ALS (Alternating Least Squares) and Content-Based Filtering using LSH (Locality Sensitive Hashing).
The system leverages the strengths of both approaches — ALS captures user preferences from historical interaction data, while LSH finds similar books based on content features — to deliver accurate and diverse recommendations.


Features:
ALS Model for collaborative filtering.

LSH Model for fast content similarity search.

Weighted hybrid scoring for final recommendations.

Handles cold start issues more effectively than single-model systems.

Scalable and efficient for large datasets.


Tech Stack:
Python

PySpark (MLlib)

Pandas 

Jupyter Notebook (for experimentation)


How to Run:
1. Clone the repository:

git clone https://github.com/yourusername/hybrid-book-recommender.git
cd hybrid-book-recommender

2. Install dependencies:
pip install -r requirements.txt


3. Train the models (if not using pre-trained):
python scripts/train_als.py
python scripts/train_lsh.py

4. Get recommendations:


Contributers:
Mansi Verma  (Github Profile [])
Vaibhavi Sanap (Github Profile [])









