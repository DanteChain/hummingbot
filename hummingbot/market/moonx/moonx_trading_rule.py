from decimal import Decimal


class MoonxTradingRule:

    def __init__(self,
                 symbol: str,
                 priceMultiplier: int,
                 priceTick: Decimal,
                 quantityMultiplier: int,
                 quantityTick: Decimal,
                 minimumQuantity: Decimal,
                 maximumQuantity: Decimal):
        self.symbol = symbol
        self.maximumQuantity = maximumQuantity
        self.minimumQuantity = minimumQuantity
        self.quantityTick = quantityTick
        self.quantityMultiplier = quantityMultiplier
        self.priceTick = priceTick
        self.priceMultiplier = priceMultiplier
