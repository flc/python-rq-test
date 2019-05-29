def split_components(smiles_list):
    # just some cpu intensive code
    i = 10000
    while i > 0:
        i**i
        i -= 1

    ret = []
    for line in smiles_list:
        smiles, meta = line.split()
        smiles = smiles.strip()
        for comp in smiles.split('.'):
            ret.append("\t".join([comp, meta]))
    return ret
