'''
The GUI_Console is the GUI for entering queries, showing results and displaying graphs
The Graph Panel is still in developing. Currently, the GUI just show an example in the Graph Panel

If press enter and the last char is ";", then end the query input,
If press ESC, clear text and continue for next query

@author: keshan
'''
from Tkinter import * #GUI package

#from PIL import Image, ImageTk #for Tkinter to support png figure

from QueryConsole import queryConsole


#Key click events for Query Input Text
def query_Text_Event(event):
    #Query input is taken once enter is pressed and query ends with char ";"
    if event.keycode == 36:
        input_Query = query_Panel_Text.get(1.0, END).strip()
        if input_Query[-1] == ";":
            queryConsole.start(input_Query, result_Panel_Text)
            query_Panel_Text.config(state=DISABLED)

    #press ESC to clear text
    if event.keycode == 9:
        query_Panel_Text.config(state=NORMAL)
        query_Panel_Text.delete("1.0", END)
        result_Panel_Text.config(state=NORMAL)
        result_Panel_Text.delete("1.0", END)
        result_Panel_Text.config(state=DISABLED)

#the root window
root = Tk()
root.title("Heft Framework Console")
root.resizable(FALSE, FALSE)


#The below section creates the Panel for query editor
query_Panel_LabelFrame = LabelFrame(root, text="Query Editor")
query_Panel_LabelFrame.grid(row=0, column=0, padx=25, pady=15)

#The below section creates the Panel for result panel
result_Panel_LabelFrame = LabelFrame(root, text="Result Panel")
result_Panel_LabelFrame.grid(row=1, column=0, padx=25, pady=15)

#The below section creates the Scroll bar for query panel
query_with_scrollx = Scrollbar(query_Panel_LabelFrame, orient=HORIZONTAL)
query_with_scrollx.grid(row=1, column=0, sticky=W+E)
query_with_scrolly = Scrollbar(query_Panel_LabelFrame)
query_with_scrolly.grid(row=0, column=1, sticky=N+S)

#The below section creates the Scroll bar for result panel
result_with_scrollx = Scrollbar(result_Panel_LabelFrame, orient=HORIZONTAL)
result_with_scrollx.grid(row=1, column=0, sticky=W+E)
result_with_scrolly = Scrollbar(result_Panel_LabelFrame)
result_with_scrolly.grid(row=0, column=1, sticky=N+S)


#The below section creates the wideget for query panel
query_Panel_Text = Text(query_Panel_LabelFrame, width=100, height=18, wrap=NONE, xscrollcommand=query_with_scrollx.set, yscrollcommand=query_with_scrolly.set)
query_with_scrollx.config(command=query_Panel_Text.xview)
query_with_scrolly.config(command=query_Panel_Text.yview)
query_Panel_Text.grid(row=0, column=0)

query_Panel_Text.bind('<Key>', query_Text_Event)

#The below section creates the wideget for result panel
result_Panel_Text = Text(result_Panel_LabelFrame, width=100, height=18, wrap=NONE, xscrollcommand=result_with_scrollx.set, yscrollcommand=result_with_scrolly.set)
result_Panel_Text.config(state=DISABLED)
result_with_scrollx.config(command=result_Panel_Text.xview)
result_with_scrolly.config(command=result_Panel_Text.yview)
result_Panel_Text.grid(row=0, column=0)

mainloop()
