clc;
clear all;
close all;

k = 4;

T1 = csvread('example1.dat');
T2 = csvread('example2.dat');

col1 = T1(:, 1);
col2 = T1(:, 2);

%1.
gr = graph(col1, col2);
adj = adjacency(gr);   

figure(1)
plot(gr)

A = full(adj); 
figure(2)
spy(A,'r'); 

%2.
D = diag(sum(A,2));
L = (D^(-0.5))*A*(D^(-0.5));

%3.
[X, eigval] = eigs(L, 100, 'SA');

%4.
Y = X./sqrt(sum(X.^2, 2));

%5.
[idx,C] = kmeans(Y, k)

%6
figure(3)
[eigvecs, eigvals] = eig(L);
plot(sort(eigvecs(:,2)))

figure(4)
[eigvecs, eigvals] = eig(D-A);
plot(sort(eigvecs(:,2)))

