import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Read the CSV
df = pd.read_csv('your_file.csv')

# Create a pivot table to get counts
pivot_df = pd.crosstab(df['Accountable Executive'], df['Onboarding Status'])

# Define colors for each status
status_colors = {
    'Fully Onboarded': 'green',
    'In-Progress': 'yellow',
    'Committed': '#013D5B',
    'Uncommitted': '#BF5547'
}

# Reorder columns to match the color mapping (if these columns exist)
ordered_columns = []
for status in ['Fully Onboarded', 'In-Progress', 'Committed', 'Uncommitted']:
    if status in pivot_df.columns:
        ordered_columns.append(status)

# Add any remaining columns that weren't in our predefined order
for col in pivot_df.columns:
    if col not in ordered_columns:
        ordered_columns.append(col)

pivot_df = pivot_df[ordered_columns]

# Create color list based on actual columns
colors = [status_colors.get(col, 'gray') for col in pivot_df.columns]

# Create the figure with background color
fig, ax = plt.subplots(figsize=(12, 8))
fig.patch.set_facecolor('#F7F3EB')
ax.set_facecolor('#F7F3EB')

# Plot the stacked bars
pivot_df.plot(kind='bar', 
              stacked=True, 
              ax=ax,
              color=colors,
              edgecolor='white',
              linewidth=1)

# Customize the chart
ax.set_xlabel('Accountable Executive', fontsize=12, fontweight='bold')
ax.set_ylabel('Count of Controls', fontsize=12, fontweight='bold')
ax.set_title('Onboarding Status by Accountable Executive', fontsize=14, fontweight='bold', pad=20)

# Rotate x-axis labels if needed
plt.xticks(rotation=45, ha='right')

# Customize legend
ax.legend(title='Onboarding Status', 
          bbox_to_anchor=(1.05, 1), 
          loc='upper left',
          frameon=True,
          fancybox=True,
          shadow=True,
          facecolor='white',
          edgecolor='gray')

# Add grid for easier reading
ax.yaxis.grid(True, linestyle='--', alpha=0.7, color='gray')
ax.set_axisbelow(True)

# Add value labels on each segment (optional)
for container in ax.containers:
    ax.bar_label(container, label_type='center', fontsize=9, weight='bold')

# Remove top and right spines for cleaner look
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Save with the same background color
plt.savefig('onboarding_status_chart.png', 
            dpi=300, 
            bbox_inches='tight',
            facecolor='#F7F3EB',
            edgecolor='none')
plt.show()
