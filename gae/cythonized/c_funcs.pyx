from cpython cimport dict, list


cpdef cy_aggregate_scores(list similarities, dict user_scores, int n=10):
    cdef int i
    cdef dict res
    cdef int j
    cdef double weight
    res = {}
    for i in range(len(similarities)):
        inters_i = similarities[i]
        weight = user_scores[inters_i['id']]
        scores = inters_i['scores']
        items = inters_i['items']
        for j in range(len(items)):
            sku = items[j]
            res.setdefault(sku, 0.0)
            res[sku] += weight * scores[j]
    return  res.items()
