

def chunks(l,n):
    '''Yield successive n-sized chunks from l. Useful for multi-processing'''
    chunk_list =[]
    for i in xrange(0, len(l), n):
        chunk_list.append(l[i:i + n])
    return chunk_list