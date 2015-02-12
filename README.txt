v1.4:
(1) gray out zero population MBs in IIA output

v1.3
(1) IIA is computed by respecting the size of meshblock, rather than the previously defined Max Quantity
(2) add a buffer size to LGA (500m default) to bring more adjacent facilities into computation

v1.2:
(1) rank pattern changes to 1 1 1 4 4 4 4 4 9 9
(2) rank iop reversely
(3) add an option for ranking iia/upg among all LGAs

v1.1: 
(1) add buffer size for LGA in IOP 
(2) remove perimeter and number of facility in IOP calculation 
(3) fix ranking bugs (equal index value will not lead to rank increase) for all 
(4) enable user login
(5) iop support more layers
(6) add rank for each index value; sort job list ASC