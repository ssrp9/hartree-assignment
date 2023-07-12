import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class JoinAndCalculate(beam.DoFn):
    def __init__(self, dataset2):
        self.dataset2 = dataset2
    
    def process(self, element):
        counter_party = element['counter_party']
        tier = self.dataset2[counter_party]
        
        legal_entity = element['legal_entity']
        rating = element['rating']
        status = element['status']
        value = element['value']
        
        if status == 'ARAP':
            arap_sum = value
            accr_sum = 0
        elif status == 'ACCR':
            arap_sum = 0
            accr_sum = value
        else:
            arap_sum = 0
            accr_sum = 0
        
        yield {
            'legal_entity': legal_entity,
            'counter_party': counter_party,
            'tier': tier,
            'max(rating by counterparty)': rating,
            'sum(value where status=ARAP)': arap_sum,
            'sum(value where status=ACCR)': accr_sum
        }

def run_pipeline():
    # Read the datasets
    with beam.Pipeline() as pipeline:
        dataset1 = pipeline | "Read dataset1" >> ReadFromText('dataset1.csv', skip_header_lines=1)
        dataset2 = pipeline | "Read dataset2" >> ReadFromText('dataset2.csv', skip_header_lines=1)
        
        # Prepare dataset2 as a dictionary for efficient lookup
        dataset2_dict = (dataset2
                         | "Split dataset2" >> beam.Map(lambda row: row.split(','))
                         | "Create dataset2 dictionary" >> beam.Map(lambda row: (row[0], row[1]))
                         )
        
        # Join and calculate the required values
        joined_data = {'dataset1': dataset1, 'dataset2': dataset2_dict} | beam.CoGroupByKey()
        calculated_data = joined_data | beam.ParDo(JoinAndCalculate(dataset2_dict))
        
        # Write the output to a file
        calculated_data | "Write output" >> WriteToText('output.csv')

if __name__ == '__main__':
    run_pipeline()
