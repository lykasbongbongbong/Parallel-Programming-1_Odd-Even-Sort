# Homework1: Odd-Even-Sort

## Goal 1
This assignment helps you get familiar with MPI by implementing a parallel odd-even sort algorithm. Experimental evaluations on your implementation are required to guide you analyze the performance and scalability of a parallel program. You are also encouraged to explore any performance optimization and parallel computing skills in order to receive a higher score.

## Problem Description
In this assignment, you are required to implement the odd-even sort algorithm using MPI. Odd-even sort is a comparison sort which consists of two main phases: even-phase and odd-phase. In each phase, processes perform compare-and-swap operations repeatedly as follows until the input array is sorted.
In even-phase, all even/odd indexed pairs of adjacent elements are compared. If the two elements of a pair are not sorted in the correct order, the two elements are swapped. Similarly, the same compare-and-swap operation is repeated for odd/even indexed pairs in odd-phase. The odd-even sort algorithm works by alternating these two phases until the input array is completely sorted.
For you to understand this algorithm better, the execution flow of odd-even sort is illustrated step by step as below: (The array is sorted in ascending order)
