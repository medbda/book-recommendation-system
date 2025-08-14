import json

# Load your JSON file
with open("top_100_books.json", "r") as file:
    data = json.load(file)

# Extract book IDs directly
book_ids = []

for book in data["data"]["books"]:
    book_id = book.get("id")
    if book_id:
        book_ids.append(book_id)

# Remove duplicates (optional)
book_ids = list(set(book_ids))

# Print results
print("Book IDs found:")
#for bid in book_ids:
print(book_ids)
