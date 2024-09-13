import requests
import os
import csv
from dataclasses import dataclass
from decimal import Decimal

    

def main():
    """
    Can you send us the list of wallet addresses users who had tokens in the following pools from July 8th to September 3rd?
    - ETH/USDC Vault on Mode: https://explorer.mode.network//address/0x42f1A7795083eeE1f804DD4D33C5e69A0F32Bca4
    - USDC/AERO Vault on Base: https://basescan.org/address/0xc005B9833deBcF5fe6cc5bC9ba4fD74Bb382ae55
    - mooBIFI-​ETH Vault on Optimism: https://optimistic.etherscan.io/address/0x57d00d036485B5fEE6A58c8763Bdc358906E6D19
    - wfrxETH/​sfrxETH vault on Fraxtal: https://fraxscan.com/address/0xcb25214EC41Ea480068638897FcBd6F1206F5521

    the plan:
    - find the address of the vaults in beefy's config
    - find the bounding start and end block using https://dm-control.beefy.com/d/xYGUuLUVz/blocks-around-a-date?orgId=1
    - holder on July 8th:
        - use the balance subgraph with a query at block for those vaults
        - filter out the zero balances
    - all people who ever held after July 8th:
        - get the list of transfers from explorers
        - filter the transfers with the bounding blocks
        - filter out the zero balances
    - merge the two lists and remove duplicates

    OP: 122399012 -> 124904611
    BASE: 16803727 -> 19309326
    MODE: 10114609 -> 12620208
    FRAXTAL: 6793045 -> 9298644
    """
    print("Hello, World!")

    for chain, vault_addr in VAULTS.items():
        start_block = BLOCKS[chain]["start"]
        end_block = BLOCKS[chain]["end"]
        balance_at_start = get_balances_at_block(chain, vault_addr, start_block)
        # print(f"\nBalances at start for {chain} vault {vault_addr}:")
        # for balance in balance_at_start:
        #     print(f"  Address: {balance.address}")
        #     print(f"  Balance: {balance.amount}")
        #     print("  " + "-"*40)

        transfers = read_transfers(chain, vault_addr)
        transfers = [t for t in transfers if start_block <= t.block_number <= end_block]
        transfers.sort(key=lambda t: t.block_number)
    
        # print(f"\nFiltered transfers for {chain} vault {vault_addr}:")
        # for transfer in transfers:
        #     print(f"  From: {transfer.from_address}")
        #     print(f"  To: {transfer.to_address}")
        #     print(f"  Amount: {transfer.value}")
        #     print(f"  Block: {transfer.block_number}")
        #     print("  " + "-"*40)

        # address -> list[int]
        balance_diffs_per_address = {}
        for transfer in transfers:
            from_address = transfer.from_address.lower()
            to_address = transfer.to_address.lower()
            amount = transfer.value

            if from_address != "0x0000000000000000000000000000000000000000":
                balance_diffs_per_address.setdefault(from_address, []).append(-amount)
            
            if to_address != "0x0000000000000000000000000000000000000000":
                balance_diffs_per_address.setdefault(to_address, []).append(amount)

        # now, for each holder in the initial list, check if we have 
        # ever reached a balance of zero, if yes, remove from the set of valid addresses
        balance_per_address = {b.address.lower(): b.amount for b in balance_at_start}
        valid_addresses = set([b.address for b in balance_at_start])
        for address, diffs in balance_diffs_per_address.items():
            if address not in valid_addresses:
                continue

            initial_balance = balance_per_address[address]
            current_balance = initial_balance
            for diff in diffs:
                current_balance += diff
                if current_balance == 0:
                    print(f"Address {address} has reached zero balance on {chain} for vault {vault_addr}")
                    valid_addresses.discard(address)
                    break

        # export
        # Export valid addresses to CSV
        csv_filename = f"{chain}_{vault_addr}_valid_addresses.csv"
        csv_path = os.path.join("output", csv_filename)
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Address'])  # Header
            for address in valid_addresses:
                writer.writerow([address])
        
        print(f"Exported valid addresses to {csv_filename}")


        print(f"Valid addresses for {chain} vault {vault_addr}: {valid_addresses}") 

    
VAULTS = {
    "mode": "0x42f1A7795083eeE1f804DD4D33C5e69A0F32Bca4",
    "optimism": "0x57d00d036485B5fEE6A58c8763Bdc358906E6D19",
    "base": "0xc005B9833deBcF5fe6cc5bC9ba4fD74Bb382ae55",
    "fraxtal": "0xcb25214EC41Ea480068638897FcBd6F1206F5521"
}

BLOCKS = {
    "optimism": {"start": 122399012, "end": 124904611},
    "base": {"start": 16803727, "end": 19309326},
    "mode": {"start": 10114609, "end": 12620208},
    "fraxtal": {"start": 6793045, "end": 9298644},
}

BALANCES_QUERY="""
query BalanceAtBlock($block_number: Int!, $token: String!, $skip: Int!, $first: Int!) {
  tokenBalances(
    block: { number: $block_number }
    skip: $skip
    first: $first
    where: { token: $token, amount_gt: 0 }
  ) {
    account {
      address: id
    }
    amount
  }
}
"""

@dataclass
class Balance:
    address: str
    amount: int

def get_balances_at_block(chain: str, vault_addr: str, block_number: int) -> list[Balance]:
    """
    Fetches token balances for a vault at a specific block on a given chain using a subgraph query.
    Returns a dictionary of wallet addresses to token balances.
    """
    subgraph_url = f"https://api.goldsky.com/api/public/project_clu2walwem1qm01w40v3yhw1f/subgraphs/beefy-balances-{chain}/latest/gn"

    token = vault_addr
    skip = 0
    first = 1000  # Adjust this value based on your needs and API limitations

    balances = []
    while True:
        response = requests.post(
            subgraph_url,
            json={
                "query": BALANCES_QUERY,
                "variables": {
                    "block_number": block_number,
                    "token": token,
                    "skip": skip,
                    "first": first
                }
            }
        )
        data = response.json()

        if "errors" in data:
            raise Exception(f"Query failed: {data['errors']}")

        token_balances = data["data"]["tokenBalances"]
        for balance in token_balances:
            balances.append(Balance(
                address=balance["account"]["address"].lower(),
                amount=int(balance["amount"])
            ))

        if len(token_balances) < first:
            break

        skip += first

    return balances


@dataclass
class Transfer:
    block_number: int
    from_address: str
    to_address: str
    value: int

def read_transfers(chain: str, vault_addr: str) -> list[Transfer]:
    """
    Reads transfers from a CSV file in the data folder for a specific chain and vault address.
    """

    # Construct the file path
    file_name = f"{chain}_{vault_addr}.csv"
    file_path = os.path.join("data", file_name)
    
    transfers = []
    
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if chain == "mode":
                transaction = Transfer(
                    block_number=int(row['BlockNumber']),
                    from_address=row['FromAddress'].lower(),
                    to_address=row['ToAddress'].lower(),
                    value=int(row['TokensTransferred'])
                )
            else:
                transaction = Transfer(
                    block_number=int(row['Blockno']),
                    from_address=row['From'].lower(),
                    to_address=row['To'].lower(),
                    value=int(Decimal(row['TokenValue'].replace(',', '')) * Decimal('1000000000000000000'))  # Convert to wei
                )
            transfers.append(transaction)        
    return transfers

if __name__ == "__main__":
    main()