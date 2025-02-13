import scrapy
import json
import os

class MeijerSpider(scrapy.Spider):
    name = 'meijer'
    
    # Starting URL for the spider
    start_urls = ['https://ac.cnstrc.com/browse/collection_id/grocery-sale?c=ciojs-client-2.42.2&key=key_GdYuTcnduTUtsZd6&i=23459c0f-0ca4-44b6-ae9f-0cd14a778b5e&s=8&us=web&page=1&num_results_per_page=52&filters%5BavailableInStores%5D=20&sort_by=relevance&sort_order=descending&fmt_options%5Bgroups_max_depth%5D=3&fmt_options%5Bgroups_start%5D=current&_dt=1739362902086']
    
    ratings_url = 'https://apps.bazaarvoice.com/api/data/statistics.json?apiversion=5.4&passkey=caiDIkUt3Gobe6Yc0ggimusvNMK2m3FKRegCwqVBIO5ks&stats=Reviews&filter=ContentLocale:en_US,en*&filter=ProductId:{}'
    
    def start_requests(self):
        for url in self.start_urls: 
            yield scrapy.Request(
                url=url,
                headers={'User-Agent': 'Mozilla/5.0'},
                callback=self.parse
            )

    def parse(self, response):
        # Parse the JSON response
        data = json.loads(response.body)
        
        # Extract product data from the JSON response
        products = data.get('response', {}).get('results', [])
        
        for product in products:
            product_data = product.get('data', {})
            # Extract relevant details
            id = product_data.get('id')
            name = product_data.get('summary')
            price = product_data.get('discountSalePriceValue')
            product_url = product_data.get('url')
            hero_image = product_data.get('image_url')
            variation = product_data.get('variation_id')

            if id and name and price and product_url and hero_image and variation:
                yield scrapy.Request(
                    url=self.ratings_url.format(id),
                    callback=self.parse_ratings,
                    meta={'product_data': {
                        'name': name,
                        'price': price,
                        'product_url': product_url,
                        'hero_image': hero_image,
                        'variation': variation,
                        'id': id
                    }}
                )

    def parse_ratings(self, response):
        product_data = response.meta['product_data']
        ratings_data = json.loads(response.body)
        
        results = ratings_data.get('Results', [])
        
        if results:
            review_stats = results[0].get('ProductStatistics', {}).get('ReviewStatistics', {})
            product_data['average_rating'] = review_stats.get('AverageOverallRating', 'No rating')
            product_data['total_reviews'] = review_stats.get('TotalReviewCount', 'No reviews')
        else:
            product_data['average_rating'] = 'No rating'
            product_data['total_reviews'] = 'No reviews'
        
        self.save_to_json(product_data) 
        yield product_data

    def save_to_json(self, data):
        filename = 'final_products.json'
        
        if os.path.exists(filename):
            with open(filename, 'r+', encoding='utf-8') as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
                existing_data.append(data)
                f.seek(0)
                json.dump(existing_data, f, indent=4)
        else:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump([data], f, indent=4)
