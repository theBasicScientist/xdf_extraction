"""
XDF Data Extraction and Visualization Tool

This module provides utilities for examining and extracting data from XDF
(Extensible Data Format) files recorded from Lab Streaming Layer (LSL) servers.
It generates interactive HTML visualizations of XDF file structure and extracts
behavioral event streams for analysis.

Author: theScientist@theBasicScientist.com
License: GNU GENERAL PUBLIC LICENSE V3
Version: 1.1.4

Usage:
    python xdf_extraction.py data.xdf
    python xdf_extraction.py data.xdf -b "Markers" -o output_name

For more information, see README.md
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Tuple, Optional
import sys
import argparse
from pathlib import Path

class XDFSchematicGenerator:
    """
    Generates hierarchical tree visualizations of XDF dictionary structure.
    Prioritizes behavioral data stream and creates clear text-based
    and interactive tree diagrams.
    """
    
    def __init__(self, streams: List[Dict[str, Any]], header: Dict[str, Any] = None,
                 behavioral_stream_name: Optional[str] = None):
        """
        Initialize with loaded XDF data from pyxdf.load_xdf()
        
        Parameters
        ----------
        streams : list of dict
            List of stream dictionaries returned by pyxdf.load_xdf()
        header : dict, optional
            Header dictionary returned by pyxdf.load_xdf()
        behavioral_stream_name : str, optional
            Name of the behavioral stream to prioritize (e.g., 'StimLabels', 'Markers', 'Events').
            If None, will search for common behavioral stream names.
        """
        self.streams = streams
        self.header = header if header is not None else {}
        self.stream_summary = None
        self.behavioral_idx = None
        self.behavioral_stream_name = behavioral_stream_name
        
        # Find behavioral stream index
        self._find_behavioral_stream()
        
    def _find_behavioral_stream(self):
        """Find the index of the behavioral stream."""
        # Common behavioral stream names to search for
        default_names = ['stimlabels', 'markers', 'events', 'triggers', 'behavioral']
        
        for idx, stream in enumerate(self.streams):
            info = stream.get('info', {})
            name = info.get('name', [''])[0] if isinstance(info.get('name'), list) else info.get('name', '')
            name_lower = name.lower()
            
            # If specific name provided, match it exactly (case-insensitive)
            if self.behavioral_stream_name:
                if self.behavioral_stream_name.lower() in name_lower:
                    self.behavioral_idx = idx
                    self.behavioral_stream_name = name  # Store actual name
                    print(f"Found behavioral stream: '{name}' at index {idx}")
                    return
            else:
                # Search for common names
                if any(default in name_lower for default in default_names):
                    self.behavioral_idx = idx
                    self.behavioral_stream_name = name
                    print(f"Auto-detected behavioral stream: '{name}' at index {idx}")
                    return
        
        if self.behavioral_idx is None:
            print("Warning: No behavioral stream found. Available streams:")
            for idx, stream in enumerate(self.streams):
                info = stream.get('info', {})
                name = info.get('name', [''])[0] if isinstance(info.get('name'), list) else info.get('name', '')
                print(f"  [{idx}] {name}")
    
    def _get_size_mb(self, obj: Any) -> float:
        """Estimate memory footprint in MB."""
        total_size = sys.getsizeof(obj)

        # For dictionaries, recursively calculate size of contents
        if isinstance(obj, dict):
            for key, value in obj.items():
                total_size += sys.getsizeof(key)
                if isinstance(value, np.ndarray):
                    # For numpy arrays, use nbytes for accurate size
                    total_size += value.nbytes
                elif isinstance(value, (list, dict)):
                    # Recursively calculate for nested structures
                    total_size += self._get_size_mb(value) * 1024 * 1024  # Convert back to bytes
                else:
                    total_size += sys.getsizeof(value)
        elif isinstance(obj, np.ndarray):
            total_size = obj.nbytes
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, np.ndarray):
                    total_size += item.nbytes
                else:
                    total_size += sys.getsizeof(item)

        return total_size / (1024 * 1024)
    
    def _get_type_and_info(self, obj: Any) -> Tuple[str, str]:
        """Get type and summary info for an object."""
        if isinstance(obj, np.ndarray):
            dtype = f"ndarray[{obj.dtype}]"
            info = f"shape {obj.shape}"
            if obj.size > 0:
                info += f", range [{np.min(obj):.2f}, {np.max(obj):.2f}]"
            return dtype, info
        elif isinstance(obj, list):
            return "list", f"{len(obj)} items"
        elif isinstance(obj, dict):
            return "dict", f"{len(obj)} keys"
        elif isinstance(obj, str):
            return "str", f'"{obj[:50]}..."' if len(obj) > 50 else f'"{obj}"'
        elif isinstance(obj, (int, float)):
            return type(obj).__name__, str(obj)
        else:
            return type(obj).__name__, str(obj)[:50]
    
    def _extract_stream_info(self, stream_idx: int, stream: Dict) -> Dict:
        """Extract key metadata from a stream."""
        info = stream.get('info', {})
        time_series = stream.get('time_series', np.array([]))
        time_stamps = stream.get('time_stamps', np.array([]))
        
        # Handle nested XML-like info structure
        def get_nested(d, key, default='N/A'):
            val = d.get(key, [default])
            return val[0] if isinstance(val, list) and len(val) > 0 else val
        
        stream_name = get_nested(info, 'name', f'Stream_{stream_idx}')
        stream_type = get_nested(info, 'type', 'Unknown')
        channel_count = int(get_nested(info, 'channel_count', 0))
        srate = float(get_nested(info, 'nominal_srate', 0))
        
        duration = 0
        if len(time_stamps) > 1:
            duration = time_stamps[-1] - time_stamps[0]
        
        return {
            'index': stream_idx,
            'name': stream_name,
            'type': stream_type,
            'channels': channel_count,
            'srate_hz': srate,
            'samples': len(time_stamps),
            'duration_sec': duration,
            'size_mb': self._get_size_mb(stream),
            'time_series_shape': time_series.shape if hasattr(time_series, 'shape') else 'N/A'
        }
    
    def generate_summary_table(self) -> pd.DataFrame:
        """Generate summary table of all streams."""
        summaries = []
        
        # Add behavioral stream first if found
        if self.behavioral_idx is not None:
            summaries.append(self._extract_stream_info(self.behavioral_idx, self.streams[self.behavioral_idx]))
        
        # Add remaining streams
        for idx, stream in enumerate(self.streams):
            if idx != self.behavioral_idx:
                summaries.append(self._extract_stream_info(idx, stream))
        
        self.stream_summary = pd.DataFrame(summaries)
        return self.stream_summary
    
    def get_behavioral_data(self) -> pd.DataFrame:
        """
        Extract behavioral stream data as DataFrame with all available columns.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing all time_series columns plus time_stamps
        """
        if self.behavioral_idx is None:
            return pd.DataFrame()
        
        stream = self.streams[self.behavioral_idx]
        time_series = stream.get('time_series', [])
        time_stamps = stream.get('time_stamps', [])
        
        df = pd.DataFrame()
        
        # Handle different time_series formats
        if isinstance(time_series, np.ndarray):
            # Multi-dimensional array - each column is a feature
            if time_series.ndim > 1:
                n_cols = time_series.shape[1]
                # Create column names based on number of features
                if n_cols == 1:
                    df["time_series"] = time_series[:, 0]
                else:
                    for i in range(n_cols):
                        df[f"feature_{i}"] = time_series[:, i]
            else:
                # 1D array - single feature
                df["time_series"] = time_series
                
        elif isinstance(time_series, list) and len(time_series) > 0:
            # List of values
            if isinstance(time_series[0], (list, np.ndarray)):
                # List of arrays/lists - each is a row with multiple features
                n_cols = len(time_series[0]) if len(time_series[0]) > 0 else 1
                
                if n_cols == 1:
                    df["time_series"] = [x[0] if len(x) > 0 else '' for x in time_series]
                else:
                    # Multiple features per sample
                    for i in range(n_cols):
                        df[f"feature_{i}"] = [x[i] if len(x) > i else '' for x in time_series]
            else:
                # Simple list of scalars or strings
                df["time_series"] = time_series
        
        # Add timestamps
        if len(time_stamps) > 0:
            df["time_stamp"] = time_stamps
        
        return df
    
    def save_behavioral_data_csv(self, output_stem: str) -> Optional[str]:
        """
        Save behavioral data to CSV file.
        
        Parameters
        ----------
        output_stem : str
            Output filename stem (without extension)
        
        Returns
        -------
        str or None
            Path to saved CSV file, or None if no behavioral data
        """
        df = self.get_behavioral_data()
        
        if df.empty:
            print("No behavioral data to save")
            return None
        
        csv_file = f"{output_stem}_behavioral_data.csv"
        df.to_csv(csv_file, index=True, index_label='sample_index')
        print(f"Behavioral data saved to: {csv_file}")
        return csv_file
    
    def _build_interactive_tree_html(self, obj: Any, name: str, level: int = 0, 
                                    max_depth: int = 6, node_id: str = "root") -> str:
        """Build interactive collapsible HTML tree."""
        if level > max_depth:
            return ""
        
        obj_type, obj_info = self._get_type_and_info(obj)
        size_mb = self._get_size_mb(obj)
        
        # Create size display
        size_str = f" | {size_mb:.2f} MB" if size_mb > 0.01 else ""
        
        # Create the node label
        label = f"{name}: <span class='type'>{obj_type}</span>"
        if obj_info:
            label += f" | <span class='info'>{obj_info}</span>"
        label += size_str
        
        html = ""
        
        # Check if this node has children
        has_children = False
        if isinstance(obj, dict) and level < max_depth:
            has_children = len(obj) > 0
        elif isinstance(obj, list) and level < max_depth:
            has_children = len(obj) > 0 and isinstance(obj[0], dict)
        
        if has_children:
            # Create collapsible node
            child_id = f"{node_id}_{name}".replace(' ', '_').replace('[', '').replace(']', '')
            html += f'<div class="tree-node level-{level}">'
            html += f'<span class="toggle" onclick="toggleNode(\'{child_id}\')">▶</span>'
            html += f'<span class="label">{label}</span>'
            html += f'<div id="{child_id}" class="children" style="display:none;">'
            
            # Add children
            if isinstance(obj, dict):
                # Prioritize certain keys
                priority_keys = ['info', 'time_series', 'time_stamps', 'footer']
                other_keys = [k for k in obj.keys() if k not in priority_keys]
                sorted_keys = [k for k in priority_keys if k in obj.keys()] + other_keys
                
                for key in sorted_keys:
                    html += self._build_interactive_tree_html(
                        obj[key], str(key), level + 1, max_depth, child_id
                    )
            elif isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
                html += self._build_interactive_tree_html(
                    obj[0], "[0]", level + 1, max_depth, child_id
                )
            
            html += '</div></div>'
        else:
            # Leaf node
            html += f'<div class="tree-node level-{level}">'
            html += f'<span class="leaf">└</span>'
            html += f'<span class="label">{label}</span>'
            html += '</div>'
        
        return html
    
    def generate_interactive_html(self, max_depth: int = 6, 
                                  output_file: str = 'xdf_interactive.html',
                                  save_behavioral_csv: bool = True) -> str:
        """
        Generate interactive HTML with collapsible tree and behavioral data table.
        
        Parameters
        ----------
        max_depth : int
            Maximum depth to traverse
        output_file : str
            Output filename
        save_behavioral_csv : bool
            If True, save behavioral data as CSV
        
        Returns
        -------
        str
            Path to output file
        """
        # Generate summary table
        summary_df = self.generate_summary_table()
        
        # Get behavioral data
        behavioral_df = self.get_behavioral_data()
        
        # Save behavioral data as CSV if requested
        csv_file = None
        if save_behavioral_csv and not behavioral_df.empty:
            output_stem = Path(output_file).stem
            csv_file = self.save_behavioral_data_csv(output_stem)
        
        # Build HTML
        html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>XDF Data Structure</title>
    <style>
        body {
            font-family: 'Courier New', monospace;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        h1 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }
        h2 {
            color: #34495e;
            border-bottom: 2px solid #95a5a6;
            padding-bottom: 5px;
            margin-top: 30px;
        }
        .tree-node {
            margin-left: 20px;
            margin-top: 2px;
        }
        .toggle {
            cursor: pointer;
            display: inline-block;
            width: 20px;
            color: #3498db;
            font-weight: bold;
        }
        .leaf {
            display: inline-block;
            width: 20px;
            color: #95a5a6;
        }
        .label {
            display: inline-block;
            padding: 2px 5px;
        }
        .type {
            color: #e74c3c;
            font-weight: bold;
        }
        .info {
            color: #27ae60;
        }
        .children {
            margin-left: 20px;
        }
        .level-0 { margin-left: 0; }
        .priority-section {
            background-color: #fff9e6;
            padding: 15px;
            border-left: 4px solid #f39c12;
            margin-bottom: 20px;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-top: 10px;
        }
        th {
            background-color: #3498db;
            color: white;
            padding: 10px;
            text-align: left;
            position: sticky;
            top: 0;
        }
        td {
            padding: 8px;
            border-bottom: 1px solid #ddd;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .table-container {
            max-height: 500px;
            overflow-y: auto;
            border: 1px solid #ddd;
            margin-top: 10px;
        }
        .summary-table {
            max-height: 300px;
            overflow-y: auto;
        }
        .note {
            background-color: #e8f4f8;
            padding: 10px;
            border-left: 4px solid #3498db;
            margin: 10px 0;
        }
        .warning {
            background-color: #fff3cd;
            padding: 10px;
            border-left: 4px solid #ffc107;
            margin: 10px 0;
        }
        .csv-link {
            background-color: #27ae60;
            color: white;
            padding: 8px 15px;
            text-decoration: none;
            border-radius: 4px;
            display: inline-block;
            margin-top: 10px;
        }
        .csv-link:hover {
            background-color: #229954;
        }
    </style>
    <script>
        function toggleNode(id) {
            var element = document.getElementById(id);
            var toggle = event.target;
            if (element.style.display === 'none') {
                element.style.display = 'block';
                toggle.textContent = '▼';
            } else {
                element.style.display = 'none';
                toggle.textContent = '▶';
            }
        }
        
        function expandAll() {
            var children = document.getElementsByClassName('children');
            var toggles = document.getElementsByClassName('toggle');
            for (var i = 0; i < children.length; i++) {
                children[i].style.display = 'block';
            }
            for (var i = 0; i < toggles.length; i++) {
                toggles[i].textContent = '▼';
            }
        }
        
        function collapseAll() {
            var children = document.getElementsByClassName('children');
            var toggles = document.getElementsByClassName('toggle');
            for (var i = 0; i < children.length; i++) {
                children[i].style.display = 'none';
            }
            for (var i = 0; i < toggles.length; i++) {
                toggles[i].textContent = '▶';
            }
        }
    </script>
</head>
<body>
    <div class="container">
        <h1>XDF Data Structure Schematic</h1>
        
        <div class="note">
            <strong>Interactive Tree:</strong> Click ▶ to expand nodes and explore the data structure. 
            Use the buttons below to expand/collapse all nodes.
        </div>
        
        <button onclick="expandAll()" style="padding: 8px 15px; margin-right: 10px; cursor: pointer;">Expand All</button>
        <button onclick="collapseAll()" style="padding: 8px 15px; cursor: pointer;">Collapse All</button>
        
        <h2>Stream Summary</h2>
        <div class="summary-table table-container">
"""
        
        # Add summary table
        html += summary_df.to_html(index=False, escape=False)
        html += """
        </div>
"""
        
        # Add behavioral data table if available
        if not behavioral_df.empty and self.behavioral_stream_name:
            n_features = len([col for col in behavioral_df.columns if col != 'time_stamp'])
            feature_info = f"Features: {n_features}, " if n_features > 1 else ""
            
            html += f"""
        <h2>{self.behavioral_stream_name} Data (Behavioral Events)</h2>
        <div class="priority-section">
            <p><strong>This table shows the behavioral markers/events recorded during the experiment.</strong></p>
            <p>{feature_info}Rows: {len(behavioral_df)}, Time span: {behavioral_df['time_stamp'].iloc[0]:.2f} - {behavioral_df['time_stamp'].iloc[-1]:.2f} seconds</p>
"""
            if csv_file:
                csv_filename = Path(csv_file).name
                html += f"""
            <a href="{csv_filename}" class="csv-link" download>📥 Download as CSV</a>
"""
            html += """
        </div>
        <div class="table-container">
"""
            html += behavioral_df.to_html(index=True, escape=False, index_names=['sample_index'])
            html += """
        </div>
"""
        elif self.behavioral_idx is None:
            html += """
        <div class="warning">
            <strong>Warning:</strong> No behavioral stream was found or specified. 
            Use the -b/--behavioral-stream argument to specify the stream name.
        </div>
"""
        
        # Add interactive tree
        html += """
        <h2>Interactive Data Structure Tree</h2>
"""
        
        # Build tree for behavioral stream first
        if self.behavioral_idx is not None:
            stream = self.streams[self.behavioral_idx]
            info = stream.get('info', {})
            name = info.get('name', [''])[0] if isinstance(info.get('name'), list) else info.get('name', '')
            
            html += f"""
        <div class="priority-section">
            <h3>Stream[{self.behavioral_idx}]: {name} (BEHAVIORAL DATA - PRIORITIZED)</h3>
"""
            html += self._build_interactive_tree_html(
                stream, f"stream[{self.behavioral_idx}]", 0, max_depth, "behavioral"
            )
            html += """
        </div>
"""
        
        # Add other streams
        html += """
        <h3>Other Streams</h3>
"""
        for idx, stream in enumerate(self.streams):
            if idx == self.behavioral_idx:
                continue
            
            info = stream.get('info', {})
            name = info.get('name', [''])[0] if isinstance(info.get('name'), list) else info.get('name', '')
            
            html += f"""
        <div style="margin-top: 20px;">
            <h4>Stream[{idx}]: {name}</h4>
"""
            html += self._build_interactive_tree_html(
                stream, f"stream[{idx}]", 0, max_depth, f"stream{idx}"
            )
            html += """
        </div>
"""
        
        html += """
    </div>
</body>
</html>
"""
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"Interactive HTML saved to: {output_file}")
        return output_file


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description='Generate interactive schematic visualization of XDF file structure',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (auto-detects behavioral stream)
  python xdf_render_lib.py data.xdf
  
  # Specify behavioral stream name
  python xdf_render_lib.py data.xdf -b "Markers"
  
  # Custom output and depth
  python xdf_render_lib.py data.xdf -o my_viz -d 8 -b "Events"
  
  # Skip CSV export
  python xdf_render_lib.py data.xdf --no-csv
        """
    )
    parser.add_argument(
        'xdf_file',
        help='Path to XDF file to visualize'
    )
    parser.add_argument(
        '-o', '--output',
        default='xdf_schematic',
        help='Output file stem (without extension, default: xdf_schematic)'
    )
    parser.add_argument(
        '-d', '--max-depth',
        type=int,
        default=6,
        help='Maximum tree depth to display (default: 6)'
    )
    parser.add_argument(
        '-b', '--behavioral-stream',
        type=str,
        default=None,
        help='Name of behavioral stream to prioritize (e.g., "Markers", "Events", "Triggers"). If not specified, will auto-detect common names.'
    )
    parser.add_argument(
        '--no-csv',
        action='store_true',
        help='Skip saving behavioral data as CSV'
    )
    
    args = parser.parse_args()
    
    # Import pyxdf here so it's only required when running as script
    try:
        import pyxdf
    except ImportError:
        print("Error: pyxdf is required. Install with: pip install pyxdf")
        sys.exit(1)
    
    # Load XDF file
    print(f"Loading XDF file: {args.xdf_file}")
    streams, header = pyxdf.load_xdf(args.xdf_file)
    
    # Generate schematic
    generator = XDFSchematicGenerator(streams, header, 
                                     behavioral_stream_name=args.behavioral_stream)
    
    # Generate interactive HTML
    output_file = f"{args.output}.html"
    generator.generate_interactive_html(
        max_depth=args.max_depth,
        output_file=output_file,
        save_behavioral_csv=not args.no_csv
    )
    
    print(f"\nDone! Open {output_file} in a web browser to explore the data structure.")


if __name__ == "__main__":
    main()
