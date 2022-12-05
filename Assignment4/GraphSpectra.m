clc;
clear all;
close all;

k = 2;

T1 = csvread('example1.dat');
T2 = csvread('example2.dat');

col1 = T2(:, 1);
col2 = T2(:, 2);

%1.
gr = graph(col1, col2);
adj = adjacency(gr);
A = full(adj); 
spy(A,'r');

figure(11);
plot(eig(adj));

%2.
D=diag(sum(A,2));           
L = D^(-1/2)*A*D^(-1/2);

L1 = D - A;

%3.
[X,~] = eigs(L,k);

%4.
Y=X./sum(X.*X,2).^(1/2);

%5.
idx = kmeans(Y, k);

%6
figure;
hold on;
graph_adj = graph(adj);
h = plot(graph_adj);
cluster_colors=hsv(k);
for i=1:k
    cluster_members=find(idx==i);
    highlight(h, cluster_members , 'NodeColor', cluster_colors(i,:))
end

figure(5)
[eigvecs, ~] = eigs(L1, k, 'SA');
plot(sort(eigvecs(:,2)))