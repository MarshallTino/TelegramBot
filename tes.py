import requests

# Replace with your actual OAuth access token
ACCESS_TOKEN = "ory_at_gcr5EXn3x_ahNR6-IK3k7_xEIAwC3SbH9ToNn_1okBs.FKuE3_19KXQaCt2CT5vDIBS-wwfebJKDTPTsRO0QdAI	"
BITQUERY_URL = "https://streaming.bitquery.io/graphql"

query = """
{
  solana(network: solana) {
    dexTrades(
      date: {since: "2024-02-01", till: "2024-02-15"}
      baseCurrency: {is: "6ZdxiLhM7rSQjKnYgxGskRX1PFzCL7KK3iYP8KZTpump"}
    ) {
      timeInterval {
        day(count: 1)
      }
      baseCurrency {
        symbol
        address
      }
      quoteCurrency {
        symbol
      }
      tradeAmount(in: USD)
      trades: count
      volume: quoteAmount
      high: quotePrice(calculate: maximum)
      low: quotePrice(calculate: minimum)
      open: minimum(of: block, get: quote_price)
      close: maximum(of: block, get: quote_price)
    }
  }
}
"""

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}",  # OAuth Token here
}

response = requests.post(BITQUERY_URL, json={"query": query}, headers=headers)

if response.status_code == 200:
    data = response.json()
    print(data)  # Print the response
else:
    print(f"Error {response.status_code}: {response.text}")
