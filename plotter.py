import matplotlib
import matplotlib.pyplot as plt
import numpy as np


def plot(labels, sales, customers):
    assert len(labels) == len(sales)
    assert len(labels) == len(customers)

    labels = labels
    sales = sales
    customers = customers

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, sales, width, label='Sales per month')
    rects2 = ax.bar(x + width/2, customers, width,
                    label='Unique customers per month	')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Total amount')
    ax.set_title('Unique customers per month + Sales per month')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    # plt.savefig("test.png")
    plt.show()
