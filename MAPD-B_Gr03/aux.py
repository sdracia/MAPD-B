#####IMPORTS###########
import dask.dataframe as dd
import matplotlib.pyplot as plt
import aux as aux
import math
import pandas as pd
import numpy as np

#####FUNCTIONS#####
def aggregate_up_down(df, hwid, metric):

    #dataset filtering+compute (small enough to fit in memory and diff has no problem with partition boundaries)
    selection=df[df['hwid']==hwid]
    selection=selection[selection['metric']==metric].compute()

    #switch column creation, the nan clause is because the first element tries to use an nonexistant "row -1" and returns NaN
    selection['switch']=selection['value'].diff().map(abs).map(lambda x: True if x==1 or math.isnan(x) else False)

    #use newly created column to filter dataframe
    selection=selection[selection['switch']]

    #compute periods length and convert from ms to minutes
    selection['times']=selection['when'].diff()/1000/60

    #shift value column by 1 (its only 1 and 0 alternated so we can invert them)
    selection['value']=selection['value'].map(lambda x: 0 if x==1 else 1)

    #extract columns, ignoring the first row which is not indicative because of the diff()
    times=list(selection['times'])[1:]
    values=list(selection['value'])[1:]

    #create the data structure to store the results
    history=[[value,time] for value,time in zip(values,times)]

    return history

def getuniquecouples(df):

    #group by hw and metric, with a random aggregation function (we only care about grouped values)
    g=df.groupby(['hwid','metric']).mean().compute()

    #get the ordered values
    hwids=list(g.index.get_level_values('hwid'))
    metrics=list(g.index.get_level_values('metric'))

    #zip the vectors and sort by hw and, secondarily, by metric
    combos=zip(hwids,metrics)
    combos=sorted(combos,key=lambda x:x[0])
    
    return combos

def find_anomaly_number(df):

    #call getuniquecouple to learn the existing couples
    zipped=getuniquecouples(df)

    #define some limits, 30 as (30+1)/60 and 60 as (60+1)/60 to include those
    #measurements exactly around 30 and 60 s that are usually some millisecond later
    limitlow=(30+1)/60
    limithigh=(60+1)/60

    for hw,metric in zipped:
            
            #print for visualization purposes
            print(f"hw: {hw} metric: {metric}")

            #call aggregate_up_down to group the periods
            agg=aux.aggregate_up_down(df,hw,metric)

            #divide the output in the intervals we are interested in
            times=[el[1] for el in agg]
            zeros=[x for x in times if x==0]
            between=[x for x in times if (x>limitlow and x<limithigh)]
            low=[x for x in times if (x>0 and x<limitlow)]

            #print results
            print(f"The instances of 1 single isolated uptime or downtime reading are: {len(zeros)}")
            print(f"The instances of uptimes or downtimes between 30 sec and 1 min are: {len(between)}")
            print(f"The instances of <30 seconds uptimes or downtimes are: {len(low)}")

def get_norm_data(df,hwid,metric,aggfunc='mean'):

    #filter the df
    selection=df[df['hwid']==hwid]
    selection=selection[selection['metric']==metric]

    #add minute column, minute is calculated from the first 'when' reading in the dataset 
    selection['minute']=selection['when'].map(lambda x: int((x - 1601510422405) / 60000),meta=('when','int64'))

    #group on minute using the right function, compute and sort 
    if aggfunc=='mean':
        g = selection.groupby(['hwid', 'metric', 'minute']).mean().compute().sort_values(by='minute')
    elif aggfunc=='max':
        g = selection.groupby(['hwid', 'metric', 'minute']).max().compute().sort_values(by='minute')
    elif aggfunc=='min':
        g = selection.groupby(['hwid', 'metric', 'minute']).min().compute().sort_values(by='minute')
    elif aggfunc=='sum':
        g = selection.groupby(['hwid', 'metric', 'minute']).sum().compute().sort_values(by='minute')
    else:
        raise ValueError('Non supported aggfunction')

    #extract the minutes values
    minutes=list(g.index.get_level_values('minute'))

    return list(g['value']),minutes

def fill_missing_min(values,minutes):

    #initialize some useful variables
    tmax=max(minutes)
    lastvalue=0
    fmin=minutes.copy()
    fvals=values.copy()

    #for every minute beginning at the first and ending at the one of the last seen value for this metric and hw
    for i in range(1,tmax):

        #if the minute is not present, add it to the minutes vector and add the last seen value to the values vector
        if i not in minutes:
            fmin.append(i)
            fvals.append(lastvalue)

        #if the minute is present, update the last seen value
        else:
            index=minutes.index(i)
            lastvalue=fvals[index]

    #zip and sort to have the vectors in chronological order
    _, filled_values = zip(*sorted(zip(fmin, fvals)))

    return list(filled_values)

def fill_missing_anomaly(anomalies, values, minutes):

    #initialize some useful variables
    tmax=int(max(minutes))
    lastvalue=0
    fmin=minutes.copy()
    fvals=values.copy()
    fanom=anomalies.copy()

    #for every minute beginning at the first and ending at the one of the last seen value for this metric and hw
    for i in range(1,tmax):

        #if the minute is not present, add it to the minutes vector, add 0 to the anomalies vector and the last seen value to the values vector
        if i not in minutes:
            fmin.append(i)
            fvals.append(lastvalue)
            fanom.append(0)
        #if the minute is present, update the last seen value
        else:
            index=minutes.index(i)
            lastvalue=fvals[index]

    #zip and sort to have the vectors in chronological order
    _, filled_values, filled_anomalies = zip(*sorted(zip(fmin, fvals, fanom)))

    return list(filled_values), list(filled_anomalies)

def get_norm_val_anom(df, hwid, metric, aggfunc='mean'):

    def is_within_intervals(value, intervals):
        
        #check if a time is in the intervals
        for interval in intervals:
            if int(interval[0]) <= value <= int(interval[1]):
                return 1
        return 0

    #get periods with aggregate_up_down
    history=aux.aggregate_up_down(df,hwid,metric)

    #filter the dataset
    selection=df[df['hwid']==hwid]
    selection=selection[selection['metric']==metric]

    #transform the periods in milliseconds (they need to be confronted with the 'when' column which is in ms)
    history_new=[int(x[1]*60000) for x in history]

    #get initial time value, because of the diff() in aggregate_up_down all periods are shifted by that value
    initial_time=selection['when'].min().compute()

    #calculate, for any anomaly interval (x<(60+1)*1000), the (beginning_time, end_time) couple.
    a=[(initial_time+sum(history_new[:i]),initial_time+sum(history_new[:i+1])) for i,x in enumerate(history_new) if x < (60+1)*1000]

    #create the minute column
    selection['minute']=selection['when'].map(lambda x: int((x - 1601510422405) / 60000),meta=('when','int64'))

    #create a new column using the is_within_intervals function, this column flags points that are inside anomalies
    selection['anomaly']=selection['when'].map(lambda x: is_within_intervals(x, a), meta=('anomaly', 'int'))

    #group by minute using the desired aggfun for value and max for anomaly (we want to keep the anomaly flag is one of the points is in an anomaly)
    agg_dict = {'value': aggfunc, 'anomaly': 'max'}
    g = selection.groupby(['hwid', 'metric', 'minute']).agg(agg_dict).compute().sort_values(by='minute')

    #extract the columns
    minutes=list(g.index.get_level_values('minute'))
    anomaly = list(g['anomaly'])
    value = list(g['value'])

    #fill the columns with the fill_missing_anomaly function
    filled_val, filled_anom = aux.fill_missing_anomaly(anomaly, value, minutes)

    time = np.arange(0,len(filled_anom))

    return time, filled_anom, filled_val

def corr_finder(engine, metric_list, window):
    
    #define lists for the correlation coefficients
    corr_coeff_pearson = []
    corr_coeff_spearman = []
    corr_coeff_kendall = []
    tot_corr_coeff = []

    #start len(metric_list) lists inside each method's list, this way later we can directly append to corr_coeff_*[j] without worrying about it existing
    for k in range(len(metric_list)):
        corr_coeff_pearson.append([])
        corr_coeff_spearman.append([])
        corr_coeff_kendall.append([])

    #calculate the global correlations
    for metric in metric_list:
        
        #create a dataframe with the two columns (motor's value, other metric's value)
        dftot = pd.DataFrame({'values_engine': list(engine.iloc[:,2]), 'values_metric': list(metric.iloc[:,1])})

        #calculate a list of correlations and append them to the tot_corr_coeff list
        corrs = [dftot.corr(method='pearson').iloc[0,1],dftot.corr(method='spearman').iloc[0,1],dftot.corr(method='kendall').iloc[0,1]]
        tot_corr_coeff.append(corrs)

    #iterate through the anomaly column
    for i in range(len(engine['Anomaly'])):

        #check if a specific record is flagged as being part of an anomaly
        if engine.iloc[i,1]:

            #extract the slice from the engine's values
            values_engine = list(engine.iloc[i-window:i+window+1,2])

            for j,metric in enumerate(metric_list):

                #extract the slice from the other metric's values
                values_metric = list(metric.iloc[i-window:i+window+1,1])

                #create the dataframe, compute correlations and append them
                df = pd.DataFrame({'values_engine': values_engine, 'values_metric': values_metric})
                corr_coeff_pearson[j].append(df.corr(method='pearson').iloc[0, 1])
                corr_coeff_spearman[j].append(df.corr(method='spearman').iloc[0, 1])
                corr_coeff_kendall[j].append(df.corr(method='kendall').iloc[0, 1])

    return corr_coeff_pearson,corr_coeff_spearman,corr_coeff_kendall,tot_corr_coeff

def check_fault(row,metric):
    return int((row[f'{metric}_6']==1) or (row[f'{metric}_7']==1) or (row[f'{metric}_8']==1))

def separate_readings(df,hwid,metric):

    #filter Dataframe
    selection=df[df['hwid']==hwid]
    selection=selection[selection['metric']==metric]

    #create the 3 columns for the bits we are interested in (6,7,8) by converting value into binary and taking specific digits
    selection[f'{metric}_6']=selection['value'].apply(lambda value: int(((bin(value)[2:]).zfill(16))[-6]),meta=('value', 'int64'))
    selection[f'{metric}_7']=selection['value'].apply(lambda value: int(((bin(value)[2:]).zfill(16))[-7]),meta=('value', 'int64'))
    selection[f'{metric}_8']=selection['value'].apply(lambda value: int(((bin(value)[2:]).zfill(16))[-8]),meta=('value', 'int64'))

    #create the minute column
    selection['minute']=selection['when'].map(lambda x: int((x - 1601510422405) / 60000),meta=('when','int64'))

    #group by the minute and use max aggregation function to avoid eliminating faults
    agg_dict = {f'{metric}_6': 'max', f'{metric}_7': 'max', f'{metric}_8': 'max'}
    g = selection.groupby(['hwid', 'metric', 'minute']).agg(agg_dict).compute().sort_values(by='minute')

    #take the columns from the computed dataframe
    minutes=list(g.index.get_level_values('minute'))
    alarm6 = list(g[f'{metric}_6'])
    alarm7 = list(g[f'{metric}_7'])
    alarm8 = list(g[f'{metric}_8'])

    #run a similar process as the fill_missing_anomaly function but for all 3 columns
    tmax=int(max(minutes))
    fmin=minutes.copy()
    fvals6=alarm6.copy()
    fvals7=alarm7.copy()
    fvals8=alarm8.copy()

    for i in range(1,tmax):
        if i not in minutes:
            fmin.append(i)
            fvals6.append(0)
            fvals7.append(0)
            fvals8.append(0)

    _, filled_a6, filled_a7, filled_a8 = zip(*sorted(zip(fmin, fvals6, fvals7, fvals8)))

    time = np.arange(0,len(filled_a7))

    new_df = pd.DataFrame({'minute': time, f'{metric}_6': filled_a6, f'{metric}_7': filled_a7, f'{metric}_8': filled_a8})
    
    #create a new column by calling the check_fault function which returns an or between the 3 bit columns (6,7,8)
    new_df['fault'] = new_df.apply(aux.check_fault,axis=1,args=(metric,))
    
    return new_df

def corr_finder_shift(alarm, metric_list, window, shift):
    
    #this function is very similar to the corr_finder function
    corr_coeff_pearson = []
    corr_coeff_spearman = []
    corr_coeff_kendall = []
    tot_corr_coeff = []

    for k in range(len(metric_list)):
        corr_coeff_pearson.append([])
        corr_coeff_spearman.append([])
        corr_coeff_kendall.append([])

    for metric in metric_list:

        #cut both vectors at the shortest's length to keep the columns of the same size
        max_length = min(len(alarm['minute']), len(metric['minute']))
        dftot = pd.DataFrame({'values_alarm': list(alarm.iloc[:max_length,1]), 'values_metric': list(metric.iloc[:max_length,1])})
        corrs = [dftot.corr(method='pearson').iloc[0,1], dftot.corr(method='spearman').iloc[0,1], dftot.corr(method='kendall').iloc[0,1]]
        tot_corr_coeff.append(corrs)

    for i in range(len(alarm['minute'])):
        if alarm.iloc[i,1]:

            #check that the beginning of the shifted window isn't going below 0 and the end of the non-shifted one isn't going over the max_lenght
            if i-window-shift<0 or i+window>len(alarm['minute']):
                for j in range(len(metric_list)):
                    corr_coeff_pearson[j].append(0)
                    corr_coeff_spearman[j].append(0)
                    corr_coeff_kendall[j].append(0)
            else:
                start_index = i-window
                end_index = i+window
                values_alarm = list(alarm.iloc[start_index:end_index+1,1])
                for j,metric in enumerate(metric_list):

                    #define the shifted indexes and take the corresponding slice on metric
                    start_shifted = start_index-shift
                    end_shifted = end_index-shift
                    values_metric = list(metric.iloc[start_shifted:end_shifted+1,1])
        
                    df = pd.DataFrame({'values_alarm': values_alarm, 'values_metric': values_metric})
                    corr_coeff_pearson[j].append(df.corr(method='pearson').iloc[0, 1])
                    corr_coeff_spearman[j].append(df.corr(method='spearman').iloc[0, 1])
                    corr_coeff_kendall[j].append(df.corr(method='kendall').iloc[0, 1])
                    
    return tot_corr_coeff,corr_coeff_pearson,corr_coeff_spearman,corr_coeff_kendall

######PLOTS######

def plot_anom_or_trend(minutes,anomalies,values, plot):

    #check plot type, anom only plots anomalies, trend only plots the whole history, both plots both
    if plot == 'anom':

        #make color invisible for non-anomalies
        colors=['none' if col == 0 else 'r' for col in anomalies]

        #scatter plot the values
        plt.scatter(minutes, values, c='none', edgecolors=colors, marker='o')

        plt.xlabel('Minute')
        plt.ylabel('State')
        plt.grid(True)
        plt.ylim(-0.5, 1.5)

        #make 2 levels
        plt.yticks([0, 1], ['Off', 'On'])

    elif plot == 'trend':

        #plot everything
        plt.scatter(minutes, values, c='none', edgecolors='b', marker='o')

        plt.xlabel('Minute')
        plt.ylabel('State')
        plt.grid(True)
        plt.ylim(-0.5, 1.5)
        plt.yticks([0, 1], ['Off', 'On'])

    elif plot == 'both':

        #make 2 subplots
        fig, axs = plt.subplots(2, 1, figsize=(5,10))

        #define colors for anomaly only plot 
        colors=['none' if col == 0 else 'r' for col in anomalies]

        axs[0].scatter(minutes, values, c='none', edgecolors=colors, marker='o')
        axs[0].set_xlabel('Minute')
        axs[0].set_ylabel('State')
        axs[0].set_ylim(-0.5, 1.5)
        axs[0].set_yticks([0, 1], ['Off', 'On'])
        axs[0].grid(True)


        axs[1].scatter(minutes, values, c='none', edgecolors='b', marker='o')
        axs[1].set_xlabel('Minute')
        axs[1].set_ylabel('State')
        axs[1].set_ylim(-0.5, 1.5)
        axs[1].set_yticks([0, 1], ['Off', 'On'])
        axs[1].grid(True)

        plt.tight_layout()
        plt.show()

def read_and_plot(file_path, correlation_type, hard, eng, avg_lim=0):
    
    #open the given file
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    #get the header with all the metrics and count how many there are
    header = lines[0].strip().split(',')
    n = len(header)

    #get the number of correlations present, it is the number of anomalies
    first_data_row = lines[1].strip().split()
    row_length = len(first_data_row)
    
    #parse data into hashmap
    data = {
        'Pearson': np.zeros((n, row_length)),
        'Spearman': np.zeros((n, row_length)),
        'Kendall': np.zeros((n, row_length))
    }
    
    #read the data and fill the hashmap
    for i in range(1, n+1):
        data['Pearson'][i-1, :] = list(map(float, lines[i].strip().split()))
    for i in range(n+1, 2*n+1):
        data['Spearman'][i-n-1, :] = list(map(float, lines[i].strip().split()))
    for i in range(2*n+1, 3*n+1):
        data['Kendall'][i-2*n-1, :] = list(map(float, lines[i].strip().split()))

    markers = ['o', 'v', '^', '<', '>', '1', '2', '3', '4', 's', 'p', '*', 'h', 'H', '+', 'x', 'D', 'd']
    
    #make a graph paying attention to all-NaN metrics, avg_lim defaults to 0 so if not given every not NaN value is plotted
    plt.figure(figsize=(10, 6))
    for i in range(n):
        if not np.isnan(data[correlation_type][i]).all():
            if np.abs(np.nanmean(data[correlation_type][i])) >= avg_lim:
                plt.plot(data[correlation_type][i], marker=markers[i % len(markers)], label=header[i], linewidth=1, markersize=3)
    
    plt.title(f'{correlation_type} Correlation, {hard}  {eng}')
    plt.xlabel('Number anomaly')
    plt.ylabel('Correlation')
    plt.grid(True)

    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.show()
