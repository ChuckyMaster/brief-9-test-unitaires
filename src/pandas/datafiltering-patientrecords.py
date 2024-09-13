import pandas as pd

# Données
df = pd.DataFrame(
    {
        'patient_id': [1, 2, 3, 4, 5],
        'age': [34, 45, 50, 20, 15],
        'department': [
            'Cardiology',
            'Neurology',
            'Orthopedics',
            'Cardiology',
            'Neurology',
        ],
    }
)


filtered_df = df[df['age'] > 30]

filtered_df
