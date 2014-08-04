import plotly.plotly as py
from plotly.graph_objs import *
import csv
import sys
from operator import itemgetter

py.sign_in("somyamohanty", "94wksibtod")

def main():
    total = len(sys.argv)

    if total < 1:
        print "Utilization: python graph_plotly.py <input_csv>"
        exit(0)

    with open(sys.argv[1], 'rU') as csvfile:
        reader = csv.reader(csvfile)
        reader.next()
        d = {}
        l = []
        for row in reader:
            d[row[0]] = int(row[1])
            l.append([row[0], int(row[1])])

        s_l = sorted(l, key=itemgetter(1), reverse=True)
        
        f_l = s_l[:50]

        other_total = sum(b[1] for b in s_l[51:])

        f_l.append(['Other', other_total])

    x_list = []
    y_list = []
    for each in f_l:
        x_list.append(each[0])
        y_list.append(each[1])


    trace1 = Bar(
        x = x_list,
        y = y_list,
        )

    data = Data([trace1])

    layout = Layout(
        title='Apps used by Geo-coded Twitter Users',
        xaxis=XAxis(
        title='Apps',
        showgrid=False,
        ),
        yaxis=YAxis(
        title='Number of Tweets',
        showline=False
        ))
    fig = Figure(data=data, layout=layout)
    plot_url = py.plot(fig, filename='source_twts')

# def main():
#     total = len(sys.argv)

#     if total < 1:
#         print "Utilization: python graph_plotly.py <input_csv>"
#         exit(0)

#     with open(sys.argv[1], 'rU') as csvfile:
#     	reader = csv.reader(csvfile)
#     	reader.next()
#     	x_list = []
#     	y_list = []
#     	text_list = []
#     	for row in reader:
#             x_list.append(int(row[2]))
#             y_list.append(int(row[3]))
#             try:
#             	county_str = str(row[0].decode("utf-8") + ' - ' + row[1])
#             except:
#             	row[0] = "".join(filter(lambda x: ord(x)<128, row[0]))
#             	print row[0]
#             	county_str = str(str(row[0]) + ' - ' + row[1])
#             text_list.append(county_str)

    

#     trace1 = Scatter(
#     	x = x_list,
#     	y = y_list,
#     	mode='markers',
#     	text=text_list
#     	)

#     data = Data([trace1])

#     layout = Layout(
#     	title='Census vs Tweets',
#     	xaxis=XAxis(
#         title='Census Population',
#         showgrid=False,
#     	),
#     	yaxis=YAxis(
#         title='Number of Tweets',
#         showline=False
#     	))
#     fig = Figure(data=data, layout=layout)
#     plot_url = py.plot(fig, filename='cen_twts_user')

if __name__ == "__main__":    
    main()