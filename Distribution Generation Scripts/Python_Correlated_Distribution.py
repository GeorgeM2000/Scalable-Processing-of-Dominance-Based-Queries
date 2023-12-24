# -*- coding: utf-8 -*-
"""
Created on Sat Nov  4 00:11:48 2023

@author: tsintzask
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def generate_correlated_distribution(num_points, num_dimensions, covariance=0.95):
    covariance_matrix = np.ones((num_dimensions,num_dimensions))
    for i in range(num_dimensions):
        for j in range(num_dimensions):
            if i!=j:
                covariance_matrix[i,j]=covariance
    points = np.random.multivariate_normal(mean=[0]*num_dimensions, cov=covariance_matrix, size=num_points)
    points -= np.min(points, axis=0)
    return points

def plot_2d_distribution(data):
    if data.shape[1] == 2:
        plt.scatter(data[:, 0], data[:, 1], s=10)
        plt.xlabel('x')
        plt.ylabel('y')
        plt.title('Correlated Distribution in 2D')
        plt.show()
    else:
        print("Data generated for", data.shape[1], "dimensions, but not plotted in 2D.")

def main():
    num_dimensions = int(input("Enter the number of dimensions (2 to 10): "))
    if num_dimensions < 2 or num_dimensions > 10:
        print("Number of dimensions must be between 2 and 10.")
        return

    num_points = 10000  # Number of data points

    data = generate_correlated_distribution(num_points, num_dimensions)
    dataframe = pd.DataFrame(data=data)

    file = 'Correlated_Data.txt'

    # Save DataFrame to a text file, separating points by commas
    dataframe.to_csv(file, sep=',', index=False, header=False)


    plot_2d_distribution(data)

if __name__ == "__main__":
    main()