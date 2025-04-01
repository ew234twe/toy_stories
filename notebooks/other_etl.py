"""
Azure is the new cloud provider. We need to collect their data
NA -> region_code: eu-west-1 (use this value)
$ -> instance_type
$.perhourspot -> spot_price
"""

PRICING_DATA_URL = r"https://azure.microsoft.com/api/v3/pricing/virtual-machines/page/linux/united-kingdom-south/?showLowPriorityOffers=false"


def main():
    pass


if __name__ == "__main__":
    main()
