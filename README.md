# Adaptive Time Series

<img src='https://github.com/rmravindran/ats/blob/main/wip.png' width='64'/> The library is in early development state. **If you have experience writing ts/statistical functions using hardware vector instructions, feel free to reach out**

## Experimental Adaptive Time Series Library (Work in Progress)

The library is dedicated to optimizing in-memory time series operations for exceptionally large data structures. Our primary aim is to offer a versatile range of operations while ensuring the core structure can adapt to diverse time series encodings, even those beyond numeric types. In its initial version, we are moving towards the following:

- Compact series representations for integers, floats, and decimals.
- Optimal implementation of statistical functions, beyond the usual aggregation functions.
- Sparse vector operations
- Sparse matrix operations

**Why develop this ?**

Coming from a C++ low-latency financial world, I have noticed that Go-land seriously lack a rich set of optimal time series function implementations. I've been on the hunt for these myself while developing other related products in Go. Typically, most time series databases (TSDBs) within the Go landscape only offer basic aggregation functions, leaving a gap in addressing the distinct requirements of streaming and block time series operations, which significantly differ from traditional OLAP functions. Creating optimal implementations of these functionalities in Go has the potential to enhance a multitude of systems, including existing TSDBs, built within this language's framework.


