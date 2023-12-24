import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def generate_normal_distribution(num_points, num_dimensions):
    points = np.random.normal(0, 1, size=(num_points, num_dimensions))
    points -= np.min(points, axis=0)
    return points

def plot_2d_distribution(data):
    if data.shape[1] == 2:
        plt.scatter(data[:, 0], data[:, 1], s=10)
        plt.xlabel('x')
        plt.ylabel('y')
        plt.title('Normal Distribution in 2D')
        plt.show()
    else:
        print("Data generated for", data.shape[1], "dimensions, but not plotted in 2D.")

def main():
    num_dimensions = int(input("Enter the number of dimensions (2 to 10): "))
    if num_dimensions < 2 or num_dimensions > 10:
        print("Number of dimensions must be between 2 and 10.")
        return

    num_points = 10000  # Number of data points

    data = generate_normal_distribution(num_points, num_dimensions)
    dataframe = pd.DataFrame(data=data)

    file = 'Normal_Data.txt'


    # Save DataFrame to a text file, separating points by commas
    dataframe.to_csv(file, sep=',', index=False, header=False)


    plot_2d_distribution(data)

if __name__ == "__main__":
    main()
