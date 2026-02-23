import math

class CPMMEngine:
    """
    Constant Product Market Maker (CPMM) Logic.
    Rule: The product of all outcome pools must remain constant (k).
    k = pool_A * pool_B * ... * pool_N
    """

    def calculate_price(self, outcome_pool, all_pools):
        """
        Calculates the current price (probability) of an outcome.
        Price = Product_of_Others / (Sum of Products)
        
        For Binary (Yes/No):
        Price_Yes = Pool_No / (Pool_Yes + Pool_No)
        """
        # For simple Binary markets, it's just share_ratio
        # This handles general cases (Binary OR Categorical)
        if len(all_pools) == 0: return 0
        
        # Simple summation calculation for probability approximation
        # Price ~ Pool_Opponent / Total_Pool
        total_liquidity = sum(all_pools)
        if total_liquidity == 0: return 0
        
        # Inverse logic: The smaller the pool (more bought), the higher the price.
        # This is a simplified probability derivation for display purposes.
        # A rigorous derivation uses the partial derivative of the cost function.
        # For this bot, we will use the standard "Weight" ratio.
        
        return outcome_pool / total_liquidity 

    def calc_binary_buy(self, investment, target_pool, other_pool):
        """
        Calculates shares received when buying into a Binary Market.
        
        Args:
            investment (float): Amount of fake cash user puts in.
            target_pool (float): Current liquidity of the outcome they want (e.g., YES).
            other_pool (float): Current liquidity of the other outcome (e.g., NO).
            
        Returns:
            shares_bought (float): How many shares user gets.
            new_target_pool (float): New pool size for YES.
            new_other_pool (float): New pool size for NO.
        """
        # 1. Calculate the Constant Product (k)
        k = target_pool * other_pool
        
        # 2. Determine new pool sizes
        # We act as if the user adds 'investment' to the OTHER pool 
        # (effectively supplying liquidity) to take from the TARGET pool.
        
        new_other_pool = other_pool + investment
        new_target_pool = k / new_other_pool
        
        # 3. The shares the user gets is the difference
        shares_out = target_pool - new_target_pool
        
        return shares_out, new_target_pool, new_other_pool

# --- TEST AREA ---
if __name__ == "__main__":
    # Simulate a market
    engine = CPMMEngine()
    
    # Market starts equal: 100 YES, 100 NO
    pool_yes = 100.0
    pool_no = 100.0
    
    print(f"--- MARKET START ---")
    print(f"YES Pool: {pool_yes} | NO Pool: {pool_no}")
    print(f"Current YES Price: {pool_no / (pool_yes + pool_no):.2f} cents")
    
    # User buys $50 worth of YES
    invest = 50.0
    shares, new_yes, new_no = engine.calc_binary_buy(invest, pool_yes, pool_no)
    
    print(f"\n--- USER BUYS ${invest} of YES ---")
    print(f"User receives: {shares:.2f} YES shares")
    print(f"New Pools -> YES: {new_yes:.2f} | NO: {new_no:.2f}")
    
    # New Price Check
    new_price = new_no / (new_yes + new_no)
    print(f"New YES Price: {new_price:.2f} cents (Price went UP!)")