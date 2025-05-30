# Cape Town Data Challenge Solution

## Quick Start

### Prerequisites
- Python 3.8+
- Git

### Installation & Execution
```bash
git clone https://github.com/JGold97/ds_code_challenge.git
cd ds_code_challenge
pip install -r requirements.txt
python scripts/main.py
```

The pipeline automatically downloads data from S3, processes all tasks, and generates outputs.

## Project Structure
```
CCT - Data Engineer Code Challenge/
├── README.md
├── requirements.txt
├── scripts/
│   ├── main.py                   # Run this to execute everything
│   ├── task1_s3_select.py        # S3 data extraction
│   ├── task2_join_data.py        # H3 spatial joining
│   ├── task3.py                  # Geographic analysis & anonymization
│   └── visuals.py                # Bonus: Interactive visualization
└── data/                         # Generated during execution
```

## Task Solutions

### Task 1: S3 Data Extraction
Downloads required datasets from the S3 bucket and simulates S3 SELECT by filtering the multi-resolution GeoJSON file to extract only resolution level 8 H3 polygons. The provided AWS credentials had limited permissions, so this approach achieves the same result locally.

**Result**: Successfully extracts 3,832 H3 level 8 polygons and validates against the reference file.

### Task 2: H3 Spatial Joining
Joins 941,634 service request records with H3 hexagonal indices using the h3 library. For requests without valid coordinates, sets the H3 index to 0 as specified.

**Results**:
- 77.4% success rate (729,270 valid joins)
- 22.6% failure rate (212,364 records, mostly missing coordinates)

**Error Threshold**: Set to 30% based on actual data quality - since 22.6% of records have missing coordinates, this represents the baseline failure rate plus a small buffer for geographic edge cases.

### Task 3: Geographic Analysis & Anonymization

**Step 1 - Geographic Filtering**
Gets Bellville South centroid using OpenStreetMap API and filters requests within "1 minute" distance. Two scenarios were considered:

- **Driving without traffic (60 km/h)** - 1 minute = 1.0 km radius *(selected)*
- **Driving with moderate traffic (30 km/h)** - 1 minute = 0.5 km radius

Selected the first scenario since municipal service crews typically drive to locations, and 60 km/h represents reasonable suburban road speeds. This gave 6,079 requests within range.

**Step 2 - Weather Data Integration**
Generates realistic 2020 wind data for Cape Town with seasonal patterns (SE winds in summer, NW in winter) and joins it with the filtered requests. Achieves 100% join success by matching timestamps to hourly weather records.

**Step 3 - Data Anonymization**
Anonymizes the data while preserving analytical value:
- **Location**: Reduced to ~500m precision using H3 level 8 centroids
- **Time**: Reduced to 6-hour windows (00:00, 06:00, 12:00, 18:00)
- **Identifiers**: Removed notification numbers, exact coordinates, and precise timestamps

The anonymization preserves neighborhood-level spatial analysis and time-of-day patterns while preventing identification of specific addresses or exact timing.

## Bonus: Interactive Visualizations

Out of curiosity, I created interactive maps to visualize the results and thought I might as well share them:

```bash
python scripts/visuals.py
```

This generates:
- **Interactive heatmap** showing service request density around Bellville South
- **H3 hexagon choropleth map** displaying request counts per anonymized spatial unit
- **Static analysis plots** with temporal patterns and wind data distributions
- 
Note: When you run the visualization script, it will first display static plots in your Python environment. The interactive maps (heatmap and choropleth) are saved as HTML files in the project directory:

data/bellville_service_requests_heatmap.html
data/bellville_h3_choropleth_map.html

Open these HTML files in your web browser to view the interactive maps.

## Key Outputs

- `data/sr_with_h3_indices.csv.gz`: All service requests with H3 indices
- `data/bellville_requests_anonymized.csv`: Final anonymized dataset (6,079 records)
- `data/bellville_requests_with_wind.csv`: Pre-anonymization data with weather
- `data/bellville_south_wind_2020.csv`: Generated wind data
- `data/bellville_service_requests_heatmap.html`: Interactive visualization
- `data/bellville_h3_choropleth_map.html`: H3 hexagon map

## Dependencies

See `requirements.txt` for full list. Main libraries:
- `pandas`, `geopandas`: Data processing
- `h3`: Spatial indexing
- `boto3`: S3 access
- `requests`: API calls
- `numpy`: Numerical calculations
- `folium`, `matplotlib`: Visualizations (for interactive maps)

## Code Quality

- Comprehensive logging with timing and progress tracking
- Data validation against provided reference files
- Error handling with appropriate thresholds
- Modular design with separate scripts per task
- Single-command execution from main.py

## Execution

Run `python scripts/main.py` to execute the complete pipeline. Total runtime is approximately 3-4 minutes depending on network speed for data downloads.
