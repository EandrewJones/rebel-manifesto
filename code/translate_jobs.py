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
        self.jobs = rem_failed_jobs(np.array(jobs))
        self.n_tokens = np.array([job['n_tokens'] for job in self.jobs])
        # TODO: needs to not be a static variable but a method or propoerty (?)
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
        
        if batch_size is None or batch_size >= np.sum(n_tokens):
            return ~self.is_translated
        
        # Find largest batch size
        batch_idx = (np.cumsum(n_tokens) <= batch_size)
        remainder = batch_size - np.sum(n_tokens[batch_idx])
        u_mask = (~batch_idx & (n_tokens <= remainder))
        add_idx = np.where(u_mask)[0][np.argmax(n_tokens[u_mask])]
        batch_idx[add_idx] = True
        
        # Prepend False for already translated docs
        if np.sum(self.is_translated) > 0:
            prepend_array = np.array([False] * np.sum(self.is_translated))
            batch_idx = np.append(prepend_array, batch_idx)
        
        return batch_idx
        
    def translate_documents(self):
        '''Translate Documents in batch'''
        
        # Get batch indices
        batch_idx = self._get_batch_idx()
        print(f'Number of documents in batch: {np.sum(batch_idx)}\n')
        
        # Translate and write files
        print('Translating document:\n')
        n_translated = 0
        for idx, job in enumerate(self.jobs):
            if idx in np.where(batch_idx)[0]:
                try:
                    job['translation'] = [self.translator.translate(p) for p in job['paragraphs']]
                    job['is_translated'] = True
                    write_file(job=job, use_translation=True, base_dir=self.save_dir)
                    self.jobs[idx] = job
                    n_translated += 1
                    print(f'\t{n_translated} / {np.sum(batch_idx)}')
                except Exception as err:
                    print(err)
                    print('\nSaving jobs and exiting.')
                    self.save()
                    break
                                
    def save(self):
        '''Overwrites job file with current jobs'''
        with open(self.job_file, 'wb') as output:
            pickle.dump(self.jobs, output, pickle.HIGHEST_PROTOCOL)


# ========= #
# Functions #
# ========= #

def write_file(job: list, use_translation: bool, base_dir: str):
    """
    Write list of paragraphs to file
    """
    document = job['translation'] if use_translation else job['paragraphs']
    file_name = job['title'] + '_' + job['doc_type'] + '_' + job['date'] + '.txt'
    if len(file_name) > 255: # clip title length
        file_name = job['title'][0:201] + '_' + job['doc_type'] + '_' + job['date'] + '.txt'
    path_to_file = os.path.join(base_dir, file_name)
    with open(path_to_file, 'w') as f:
        f.writelines("%s\n" % p for p in document) 

def del_list_numpy(l: list, id_to_del: list) -> list:
    '''Delete items indexed by id_to_del from list l.'''
    arr = np.array(l)
    return list(np.delete(arr, id_to_del))

def rem_failed_jobs(jobs):
    """Removes jobs without any paragraph"""
    has_paragraphs = np.array(['paragraphs' in job.keys() for job in jobs])
    return jobs[has_paragraphs]
    
# ====== #
# Script #
# ====== #
        
if __name__ == '__main__':
    
    # Script Header
    dash = '-' * 70
    print('\n' + dash)
    print('{:-^70}'.format(' Batch Translation via Free Google Translator API '))
    print('{:-^70}'.format(' ' + time.ctime() + ' '))
    print(dash + '\n')
    
    # Get Arguments
    parser = argparse.ArgumentParser(description='Translate Web Scraping Jobs to English in Batches using Google Translator Free API.')
    parser.add_argument('-s', '--save_dir', type=str, required=True, help='The directory where translated docs will be saved.')
    parser.add_argument('-j', '--job_file', type=str, required=True, help='Full path to .pkl file containing jobs to be translated.')
    parser.add_argument('-b', '--batch', type=int, default='-1', help='Number of tokens to translate.')
    args = parser.parse_args()
    
    # Instantiate Batch Translator
    print(f'Reading jobs from: {args.job_file}\nSaving translated documents to: {args.save_dir}\nBatch size: {args.batch}\n')
    batch_translator = BatchDocumentTranslator(save_dir=args.save_dir, job_file=args.job_file, batch=args.batch)
    
    # Get Batch
    print(f'There are {batch_translator.__len__()} documents.')
    print(f'\tNumber untranslated: {batch_translator.len_untranslated_jobs()}')
    print(f'\tNumber translated: {batch_translator.len_translated_jobs()}\n')
    
    if batch_translator.__len__() == batch_translator.len_translated_jobs():
        print('All jobs translated. Exiting.')
        exit(0)
    
    # Translate documents
    batch_translator.translate_documents()
    
    # Print new totals
    print(f'\tNumber untranslated: {batch_translator.len_untranslated_jobs()}')
    print(f'\tNumber translated: {batch_translator.len_translated_jobs()}\n')
    
    # Save and exit
    batch_translator.save()
    