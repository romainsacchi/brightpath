# BrightPath
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

BrightPath is a Python library designed to convert Brightway LCA inventories into a format that can be read and imported into Simapro.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install BrightPath.

```bash

    pip install brightpath
    
```

## Usage

```python

   import brightpath
   
   # Create a converter object with the path to the Brightway LCA inventory
   converter = brightpath.Converter('path_to_inventory')
   
   # Convert the inventory to a format compatible with Simapro 9.x
   sima_inventory = converter.to_simapro()
   
   # Save the converted inventory to a file
   sima_inventory.to_file('output_path')

```

## License

[BSD-3-Clause](https://github.com/romainsacchi/brightpath/blob/master/LICENSE).

## Contributing

See [contributing](https://github.com/romainsacchi/brightpath/blob/master/CONTRIBUTING.md).

