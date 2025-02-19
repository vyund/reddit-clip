import os
import numpy as np
import pandas as pd
import glob
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from datetime import datetime

PLOT = False

def image_equal(img_path1, img_path2):
    img1 = mpimg.imread(img_path1)
    img2 = mpimg.imread(img_path2)

    return np.array_equal(img1, img2)

if __name__ == '__main__':
    data_dir = '../data/'
    csv_files = glob.glob(data_dir + '*.csv')

    master_id = datetime.now().strftime('%Y-%m-%d_%H-%M')
    master_dir = '../master_data/'
    os.makedirs(master_dir, exist_ok=True)

    df_list = [pd.read_csv(file) for file in csv_files]
    merged_df = pd.concat(df_list, ignore_index=True)

    duplicates = merged_df[merged_df.duplicated(subset='Title', keep=False)]
    grouped_duplicates = duplicates.groupby('Title')

    to_remove = []

    for key, group in grouped_duplicates:
        duplicate_paths = group['Image_Filename'].reset_index()

        if PLOT:
            fig, axes = plt.subplots(nrows=1, ncols=len(duplicate_paths))

            for i in range(len(duplicate_paths)):
                img = mpimg.imread(duplicate_paths.iloc[i].Image_Filename)
                axes[i].imshow(img)
                axes[i].axis('off')
            
            plt.tight_layout()
            plt.show()
        else:
            for i in range(len(duplicate_paths)):
                for j in range(i + 1, len(duplicate_paths)):
                    if image_equal(duplicate_paths.iloc[i].Image_Filename, duplicate_paths.iloc[j].Image_Filename):
                        to_remove.append(duplicate_paths.iloc[i].values[0])

    filtered_df = merged_df.drop(index=to_remove)
    filtered_df.to_csv(f'{master_dir}master_{master_id}.csv', index=False)
