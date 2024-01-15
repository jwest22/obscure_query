from openai import OpenAI

class callOpenAI:
    def __init__(self):
        self.client = OpenAI()

    def api_call_query(self, relation_map, question):
        response = self.client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "user",
                    "content": f"""
                    Please write a SQL query using the database schema: [{relation_map}], to answer the following question: "{question}". 
                    Please do not include anything other then the SQL query in your response. Ensure all joins, table references and column references match the provided schema.
                    """
                }
            ],
            temperature=1,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        return response
