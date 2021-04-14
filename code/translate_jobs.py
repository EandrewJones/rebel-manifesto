import os
import google_trans_new
import pickle
import time
import argparse
import numpy as np


class BatchDocumentTranslator(object):
    
    def __init__(self, save_dir, job_file, batch):
        # Store I/O locations
        self.save_dir = save_dir
        self.job_file = job_file
        
        # Load jobs
        jobs = pickle.load(open(self.job_file, 'rb'))
        self.jobs = np.array(jobs) 
        self.n_tokens = np.array([job['n_tokens'] for job in self.jobs])
        self.is_translated = np.array([job['is_translated'] for job in self.jobs])
        
        # Instantiate translator
        self.translator = google_trans_new.google_translator()
        
        # Determine batchsize
        self.batch = None
        if batch != -1:
            self.batch = batch
            
    def __len__(self):
        return len(self.jobs)
    
    def len_translated_jobs(self):
        return np.sum(self.is_translated)
    
    def len_untranslated_jobs(self):
        return np.sum(~self.is_translated)
    
    def get_translated_jobs(self):
        return self.jobs[self.is_translated]
    
    def get_untranslated_jobs(self):
        return self.jobs[~self.is_translated]
        
    def _get_batch_idx(self):
        """Creates a batch of jobs as close to specified batchsize as possible"""

        # Get parameters from self
        batch_size = self.batch
        untranslated_jobs = self.get_untranslated_jobs()
        n_tokens = self.n_tokens[~self.is_translated]
        
        # TODO if batch is none return all ~is_translated
        # TODO if batch > np.sum(self.n_tokens)
        if batch_size is None or batch_size >= np.sum(n_tokens):
            return ~self.is_translated
        
        # Find largest batch size
        batch_idx = (np.cumsum(n_tokens) <= batch_size)
        remainder = batch_size - np.sum(n_tokens[batch_idx])
        u_mask = (~batch_idx & (n_tokens <= remainder))
        add_idx = np.where(u_mask)[0][np.argmax(n_tokens[u_mask])]
        batch_idx[add_idx] = True
        
        return batch_idx
        
    def translate_documents(self):
        '''Translate Documents in batch'''
        
        # Get batch indices
        batch_idx = self._get_batch_idx()
            
        # Translate and write files
        for idx, job in enumerate(self.jobs):
            if idx in np.where(batch_idx)[0]:
                try:
                    job['paragraphs'] = self.translator.translate(job['paragraphs'])
                    job['is_translated'] = True
                    write_file(job=job, base_dir=self.save_dir)
                    self.jobs[idx] = job
                except (RuntimeError, TypeError, NameError, ValueError) as err:
                    print('Error: ', err)
                                
    def save(self):
        '''Overwrites job file with current jobs'''
        with open(self.job_file, 'wb') as output:
            pickle.dump(self.jobs, output, pickle.HIGHEST_PROTOCOL)
        
# ========= #
# Functions #
# ========= #

def write_file(job: list, base_dir):
    """
    Write list of paragraphs to file
    """
    file_name = job['title'] + '_' + job['doc_type'] + '_' + job['date'] + '.txt'
    path_to_file = os.path.join(base_dir, file_name)
    with open(path_to_file, 'w') as f:
        f.writelines("%s\n" % p for p in job['paragraphs']) 

        
if __name__ == '__main__':
    # Get Arguments
    parser = argparse.ArgumentParser(description='Translate Web Scraping Jobs to English in Batches using Google Translator Free API.')
    parser.add_argument('-s', '--save_dir', type=str, required=True, help='The directory where translated docs will be saved.')
    parser.add_argument('-j', '--job_file', type=str, required=True, help='Full path to .pkl file containing jobs to be translated.')
    parser.add_argument('-b', '--batch', type=int, default='-1', help='Number of tokens to translate.')
    args = parser.parse_args()
    
    # Instantiate Batch Translator
    batch_translator = BatchDocumentTranslator(save_dir=args.save_dir, job_file=args.job_file, batch=args.batch)
    
    # Translate Batch
    print(f'There are {batch_translator.__len__()} documents.')
    print(f'\tNumber untranslated: {batch_translator.len_untranslated_jobs()}')
    print(f'\tNumber translated: {batch_translator.len_translated_jobs()}')
    
    if batch_translator.__len__() == batch_translator.len_translated_jobs():
        print('All jobs translated. Exiting.')
        exit(0)
    
    batch_translator.translate_documents()
    batch_translator.save()
    