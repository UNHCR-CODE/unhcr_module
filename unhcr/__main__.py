"""
Overview
    This file (__main__.py) serves as the entry point for the unhcr module. Its primary purpose is to indicate that the module has been 
    loaded successfully when executed directly. It contains a simple main function that prints a confirmation message.

Key Components
    main() function: 
        This function is the core of the file. It simply prints the message "UNHCR module loaded successfully!" to the console.

if __name__ == '__main__': block: 
    This standard Python construct ensures that the main() function is executed only when the script is run directly (e.g., python -m unhcr). 
    This prevents the main() function from being called if the module is imported as a dependency in another script.
"""

import logging


def main():
    logging.info("UNHCR module loaded successfully!")


if __name__ == "__main__":
    main()
