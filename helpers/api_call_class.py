from openai import OpenAI

class callOpenAI:
    def __init__(self):
        self.client = OpenAI()

    def api_call_query(self, relation_map, question):
        response = self.client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=[
                {
                    "role": "user",
                    "content": f"""
                    Please write a BigQuery SQL query using the database schema: [{relation_map}], to answer the following question: "{question}". 
                    Please do not include anything other then the SQL query in your response. Do not include markdown.
                    Ensure the SQL query would not return an error based on the provided database schema.
                    When possible, always use human names over id fields in the select statements. 
                    """
                }
            ],
            temperature=1,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        return response
