import math
import random

class g_var:
    index = 0
    booking_info = {}
    global ref_no_issued
    ref_no_issued = {}

    def __init__(self) -> None:
        pass

    def get_ref_no():
        random_sets = "ABCabd123"
        i=0
        ref_no = ""
        while i < 8:
            ref_no += str(random.randint(0, 9))
            i+=1
            
            if i == 8:
                try:
                    ref_no_issued[ref_no]
                except:
                    pass
            else:
                ref_no_issued[ref_no] = 1

        return ref_no